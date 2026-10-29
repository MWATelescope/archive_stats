import json
import logging
import multiprocessing as mp
import random
import subprocess

from boto3.session import Session

from core import bytes_to_terabytes

logger = logging.getLogger("archive_stats")


# ---------------------------------------------------------------------------
# S3 / MinIO helpers
# ---------------------------------------------------------------------------
def get_s3_resource(profile, endpoint_url):
    """Given a profile and endpoint_url return a resource"""
    session = Session(profile_name=profile)
    return session.resource("s3", endpoint_url=endpoint_url)


def run_mc_du(profile: str, bucket_name: str, minio_path: str) -> int:
    """Runs mc and appends output to filename"""
    cmd = f"{minio_path} du {profile}/{bucket_name} --json"

    logger.info(f"{cmd}...")

    json_output = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        shell=True,
    ).stdout.decode("utf-8")

    mc_output = json.loads(json_output)

    # Example output:
    # ./mc du banksia/ingesttest --json
    # {
    # "prefix": "ingesttest",
    # "size": 8589934592,
    # "objects": 1,
    # "status": "success",
    # "isVersions": false
    # }
    size_bytes = int(mc_output["size"])

    logger.info(f"{cmd} == {size_bytes} bytes {bytes_to_terabytes(size_bytes):.3f} TB")

    return size_bytes


def get_acacia_usage(profile, endpoint_url, minio_path) -> int:
    """
    Returns the bytes used from the S3 endpoint
    """
    cpu_count = mp.cpu_count()
    logger.info(f"Setting number of simultaneous mc processes to {cpu_count}.")

    s3_resource = get_s3_resource(profile, endpoint_url)

    bucket_list = [bucket.name for bucket in s3_resource.buckets.all()]
    values = [(profile, bucket, minio_path) for bucket in bucket_list]

    with mp.Pool(cpu_count) as pool:
        results = pool.starmap(run_mc_du, values)

    return sum(results)


def randomise_banksia_vss_url(banksia_vss_endpoints: list) -> str:
    """Randomly chooses a VSS from the list of possible ones read from the config file"""
    vss = random.randint(0, len(banksia_vss_endpoints) - 1)
    return banksia_vss_endpoints[vss]


def randomise_banksia_vss_profile(banksia_vss_endpoints: list) -> str:
    """Randomly chooses a VSS from the list of possible ones read from the config file
    and then return "banksiaN" where N is the vss-N from the URL of the randomly
    chosen URL (13 character in the URL)"""
    vss = random.randint(0, len(banksia_vss_endpoints) - 1)
    # Get the vss number- from: https://vss-1.pawsey.org.au:9000 it would be 1.

    return f"banksia{banksia_vss_endpoints[vss][12]}"


def get_banksia_usage(aws_profile, endpoint_urls: list, minio_path):
    """
    Returns the bytes used from the S3 endpoint
    as DMF, banksia

    aws_profile is a profile in ~/.aws/config
    not to be confused with the minIO client profile which is
    in .mc/config.json
    """
    cpu_count = mp.cpu_count()
    logger.info(f"Setting number of simultaneous mc processes to {cpu_count}.")

    s3_resource = get_s3_resource(aws_profile, randomise_banksia_vss_url(endpoint_urls))

    bucket_list = [bucket.name for bucket in s3_resource.buckets.all()]
    dmf_buckets = []
    banksia_buckets = []

    for bucket in bucket_list:
        if (
            ("mwa01fs" in bucket)
            or ("mwa02fs" in bucket)
            or ("mwa03fs" in bucket)
            or ("mwa04fs" in bucket)
            or ("volt01fs" in bucket)
        ):
            dmf_buckets.append(bucket)
        elif "mwaingest" in bucket:
            banksia_buckets.append(bucket)
        else:
            logger.info(f"Skipping bucket {bucket}")

    # here we are creating a list of tuples to pass to the mp.Pool to execute
    # in THIS case the "profile" is a MinIO profile! In mc, the profile defines
    # both the credentials AND the endpoint, so here we want to randomise the profile
    # so we run mc against different VSS's so we don't kill Banksia!
    dmf_values = [(randomise_banksia_vss_profile(endpoint_urls), bucket, minio_path) for bucket in dmf_buckets]
    banksia_values = [(randomise_banksia_vss_profile(endpoint_urls), bucket, minio_path) for bucket in banksia_buckets]

    with mp.Pool(cpu_count) as pool:
        banksia_results = pool.starmap(run_mc_du, banksia_values)

    with mp.Pool(cpu_count) as pool:
        dmf_results = pool.starmap(run_mc_du, dmf_values)

    return sum(banksia_results) + sum(dmf_results)
