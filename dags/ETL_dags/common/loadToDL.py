import boto3


class LoadToDL:
    def __init__(self, aws_access_key_id, aws_secret_access_key, s3_bucket):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket = s3_bucket

    def create_boto3_object(self):
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        return s3

    def load_to_s3(self, s3, key, binary_file):
        s3.Bucket(self.s3_bucket).put_object(Key=key, Body=binary_file)
