import argparse
import re
from contextlib import contextmanager
from functools import partial
from time import time

import boto3
from botocore.config import Config
import s3mothball.commands
from smart_open import open, parse_uri


# set up boto clients
config = Config(retries={'max_attempts': 10, 'mode': 'standard'})
s3_client = boto3.client('s3', config=config)
s3_resource = boto3.resource('s3')


# helpers

def parse_s3_url(url):
    source_path_parsed = parse_uri(url)
    return source_path_parsed.bucket_id, source_path_parsed.key_id


def delete_file(url):
    bucket, key = parse_s3_url(url)
    s3_client.delete_object(Bucket=bucket, Key=key)


def iter_prefixes(url):
    bucket, key = parse_s3_url(url)
    key = key.rstrip('/')
    if key:
        key += '/'
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=key)
    for page in pages:
        for item in page.get('CommonPrefixes', []):
            yield (item['Prefix'].rstrip('/'))
        for item in page.get('Contents', []):
            yield (item['Key'])


def iter_objects(url):
    bucket, key = parse_s3_url(url)
    bucket = s3_resource.Bucket(bucket)
    key = key.rstrip('/')
    if key:
        key += '/'
    return bucket.objects.filter(Prefix=key)


def iter_unfinished_jobs(queue):
    batch_client = boto3.client('batch', config=config)
    for status in ('SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING'):
        for page in batch_client.get_paginator('list_jobs').paginate(jobQueue=queue, jobStatus=status):
            yield from page.get('jobSummaryList', [])


def safe_job_name(s):
    return re.sub(r'[^a-zA-Z0-9\-_]', '_', s)


def exists(path):
    """
        Check for file/URL existence with smart_open.
        Recommended method at https://github.com/RaRe-Technologies/smart_open/issues/303
    """
    try:
        with open(path):
            return True
    except IOError:
        return False


def file_must_exist(parser, path):
    if not exists(path):
        parser.exit(1, "File does not exist: %s" % path)


def file_must_not_exist(parser, path):
    if exists(path):
        parser.exit(1, "File already exists: %s" % path)


@contextmanager
def do_step(step_name, next_step_name, args, parser):
    bucket, key = parse_s3_url(args.source_url)
    in_file = "s3://harvard-cap-attic/steps/%s/%s/%s" % (step_name, bucket, key)
    out_file = "s3://harvard-cap-attic/steps/%s/%s/%s" % (next_step_name, bucket, key)
    manifest_path = "s3://harvard-cap-attic/indexes/%s/%s.tar.csv" % (bucket, key)
    tar_path = "s3://harvard-cap-attic/files/%s/%s.tar" % (bucket, key)

    file_must_exist(parser, in_file)
    file_must_not_exist(parser, out_file)

    print("Running %s for %s:\n- in_file: %s\n- out_file: %s\n- manifest_path: %s\n- tar_path: %s" % (step_name, args.source_url, in_file, out_file, manifest_path, tar_path))
    yield bucket, key, in_file, out_file, manifest_path, tar_path

    print("Removing %s and creating %s" % (in_file, out_file))
    delete_file(in_file)
    with open(out_file, 'w') as f:
        f.write(str(int(time())))

# steps

def do_unglacier(args, parser):
    with do_step('unglacier', 'archive', args, parser):
        for obj in iter_objects(args.source_url):
            bucket = obj.bucket_name
            key = obj.key
            print("Restoring %s/%s" % (bucket, key))
            response = s3_client.head_object(Bucket=bucket, Key=key)
            if 'Restore' in response:
                print("- skipping, restore in progress: ", response['Restore'])
                continue
            if response.get('StorageClass') not in ('GLACIER', 'DEEP_ARCHIVE'):
                print("- skipping, not in glacier: ", response.get('StorageClass'))
                continue
            s3_client.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest={
                    'Days': 30,
                    'GlacierJobParameters': {
                        'Tier': 'Bulk'
                    },
                }
            )
            print("- restore called")


def do_archive(args, parser):
    with do_step('archive', 'delete', args, parser) as (bucket, key, in_file, out_file, manifest_path, tar_path):
        s3mothball.commands.main(['archive', args.source_url, manifest_path, tar_path, '--strip-prefix', 'from_vendor/'])


def do_delete(args, parser):
    with do_step('delete', 'deleted', args, parser) as (bucket, key, in_file, out_file, manifest_path, tar_path):
        s3mothball.commands.main(['delete', manifest_path, tar_path, '--force-delete'])


# C&C

def queue_step(step, args, parser):
    check_timestamp = step == 'archive'
    existing_job_names = set(j['jobName'] for j in iter_unfinished_jobs(args.job_queue))
    for i, obj in enumerate(iter_objects('s3://harvard-cap-attic/steps/%s' % step)):
        if check_timestamp:
            with open("s3://%s/%s" % (obj.bucket_name, obj.key)) as f:
                timestamp = int(f.read())
            if timestamp > time() - 60*60*24:
                print("Skipping s3://%s/%s as too new" % (obj.bucket_name, obj.key))
                continue
        key = obj.key
        source_path = key.split('steps/%s/' % step, 1)[1]
        job_name = safe_job_name('%s-%s' % (step, key))

        # skip if job is already queued
        if job_name in existing_job_names:
            print("- skipping %s, already queued" % job_name)
            continue

        print("- %s: %s" % (job_name, source_path))

        # queue job
        batch_client = boto3.client('batch', config=config)
        batch_client.submit_job(
            jobName=job_name,
            jobQueue=args.job_queue,
            jobDefinition=args.job_definition,
            containerOverrides={
                'command': ('mothball_pipeline', step, 's3://' + source_path),
            },
        )

        if args.limit and i >= args.limit-1:
            break

# main

def main():
    parser = argparse.ArgumentParser(description='Control mothball process')
    subparsers = parser.add_subparsers()

    # C&C commands
    for command in ('unglacier', 'archive', 'delete'):
        create_parser = subparsers.add_parser('queue_'+command)
        create_parser.add_argument('job_queue')
        create_parser.add_argument('job_definition')
        create_parser.add_argument('--limit', type=int, default=1, help='Max number of jobs to queue; 0 for unlimited')
        create_parser.set_defaults(func=partial(queue_step, command))

    # unglacier
    create_parser = subparsers.add_parser('unglacier')
    create_parser.add_argument('source_url')
    create_parser.set_defaults(func=do_unglacier)

    # archive
    create_parser = subparsers.add_parser('archive')
    create_parser.add_argument('source_url')
    create_parser.set_defaults(func=do_archive)

    # delete
    create_parser = subparsers.add_parser('delete')
    create_parser.add_argument('source_url')
    create_parser.set_defaults(func=do_delete)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args, parser)
    else:
        parser.print_help()
        parser.exit()
