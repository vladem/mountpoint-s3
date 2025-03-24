docker run -it --rm \
    --mount type=bind,source=/home/vlaad/local/mountpoint-s3,target=/mountpoint \
    builder-centos8 \
    --expected-version 1.15.0
