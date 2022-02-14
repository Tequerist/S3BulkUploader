import os
import argparse
import time
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from queue import Queue
from threading import Thread, Event


class S3UploaderException(Exception):
    pass


def get_client(region):
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name=region
    )


def get_queue(directory: str, base_path):
    queue = Queue()
    for dir_path, dir_names, filenames in os.walk(directory):
        for filename in filenames:
            object_key = os.path.join(
                dir_path.replace(directory, base_path, 1) if base_path else dir_path,
                filename
            ).replace(os.sep, '/')
            filepath = os.path.join(dir_path, filename)
            queue.put((filepath, object_key))
            print('discovered {} files'.format(queue.qsize()), end='\r')
    return queue


def put_to_s3(run_event: Event, client, queue: Queue, bucket, acl, remove_files):
    while not queue.empty() and run_event.is_set():
        filepath, object_key = queue.get()
        try:
            client.upload_file(
                filepath, bucket, object_key,
                ExtraArgs={'ACL': acl}
            )
        except ClientError as e:
            print('Error occurred while uploading: {}'.format(str(e)))
            continue
        if remove_files:
            os.remove(filepath)
        print('uploaded: {}\nkey: {}\n{}\n'.format(
            filepath,
            object_key,
            'removed: {}'.format(filepath) if remove_files else ''
        ))


def generate_threads(
        run_event: Event,
        directory: str,
        bucket,
        region,
        acl,
        remove_files,
        base_path,
        thread_no
):
    client = get_client(region)
    queue = get_queue(directory, base_path)
    threads = []
    for i in range(thread_no):
        threads.append(Thread(
            target=put_to_s3,
            args=(run_event, client, queue, bucket, acl, remove_files)
        ))
    return threads


def start_threads(threads):
    for thread in threads:
        thread.start()


def has_live_threads(threads):
    return True in [t.is_alive() for t in threads]


def main():
    start_time = time.time()
    parser = argparse.ArgumentParser()
    run_event = Event()
    run_event.set()
    parser.add_argument('directory', help='Directory to upload')
    parser.add_argument('bucket', help='AWS S3 bucket name')
    parser.add_argument('region', help='AWS S3 region')
    parser.add_argument('--env_file', help='Env file with AWS_KEY and AWS_SECRET', default='.env')
    parser.add_argument('--acl', help='ACL Policy to be applied', default='public-read')
    parser.add_argument('--base_path', help='Base path name for object key')
    parser.add_argument('--remove_files', action='store_true', help='Delete files after uploading', default=False)
    parser.add_argument('--threads', help="No. of threads", default=5, type=int)
    args = parser.parse_args()
    try:
        if not os.path.isdir(args.directory):
            raise S3UploaderException('Directory \'{}\'does not exists'.format(args.directory))
        if not os.path.isfile(args.env_file):
            raise S3UploaderException('Env file {} does not exists'.format(args.env_file))
        if args.threads < 1:
            raise S3UploaderException('At least one thread is required')
        load_dotenv(args.env_file)
        threads = generate_threads(
            run_event,
            args.directory,
            args.bucket,
            args.region,
            args.acl,
            args.remove_files,
            args.base_path,
            args.threads
        )
        start_threads(threads)
        while has_live_threads(threads):
            try:
                [t.join(1) for t in threads
                 if t is not None and t.is_alive()]
            except KeyboardInterrupt:
                print('Please wait! gracefully stopping...')
                run_event.clear()
    except S3UploaderException as e:
        print('Error: ', str(e))

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
