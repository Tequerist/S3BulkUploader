import os
import argparse
import boto3
from tqenv import load_env, get_ev
from botocore.exceptions import ClientError


class S3UploaderException(Exception):
    pass


def get_client(region):
    return boto3.client(
        's3',
        aws_access_key_id=get_ev('AWS_KEY'),
        aws_secret_access_key=get_ev('AWS_SECRET'),
        region_name=region
    )


def upload_objects(directory: str, bucket, region, acl, remove_files, base_path):
    client = get_client(region)
    for dir_path, dir_names, filenames in os.walk(directory):
        for filename in filenames:
            object_key = os.path.join(
                dir_path.replace(directory, base_path, 1) if base_path else dir_path,
                filename
            ).replace(os.sep, '/')
            filepath = os.path.join(dir_path, filename)
            try:
                client.put_object(
                    ACL=acl,
                    Bucket=bucket,
                    Key=object_key,
                    Body=filepath
                )
            except ClientError as e:
                print('Error occurred while uploading: {}'.format(str(e)))
                continue
            print('uploaded: {}'.format(filepath))
            if remove_files:
                os.remove(filepath)
                print('removed: {}'.format(filepath))
            print('key: {}\n'.format(object_key))



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', help='Directory to upload')
    parser.add_argument('bucket', help='AWS S3 bucket name')
    parser.add_argument('region', help='AWS S3 region')
    parser.add_argument('--env_file', help='Env file with AWS_KEY and AWS_SECRET', default='.env')
    parser.add_argument('--acl', help='ACL Policy to be applied', default='public-read')
    parser.add_argument('--base_path', help='Base path name for object key')
    parser.add_argument('--remove_files', action='store_true', help='Delete files after uploading', default=False)
    args = parser.parse_args()
    try:
        if not os.path.isdir(args.directory):
            raise S3UploaderException('Directory \'{}\'does not exists'.format(args.directory))
        if not os.path.isfile(args.env_file):
            raise S3UploaderException('Env file {} does not exists'.format(args.env_file))
        load_env(args.env_file)
        upload_objects(
            args.directory,
            args.bucket,
            args.region,
            args.acl,
            args.remove_files,
            args.base_path
        )
    except S3UploaderException as e:
        print('Error: ', str(e))


if __name__ == "__main__":
    main()
