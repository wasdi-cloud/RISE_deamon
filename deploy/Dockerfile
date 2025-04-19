FROM ubuntu:24.10


LABEL org.opencontainers.image.authors="WASDI SARL"


# Labels used to search our containers and/or images
LABEL docker-image-name=rise-deamon


# From now, all commands executed are executed with the user 'root'
USER root


# This environment variable is setted to avoid packages to ask a confirmation
ENV DEBIAN_FRONTEND="noninteractive"

# This variable is setted to make Python stop creating files '.pyc'
ENV PYTHONDONTWRITEBYTECODE="False"

# This variable is setted to force the 'print' method of Python to flush message directly
ENV PYTHONUNBUFFERED="True"

# This variable is setted to set the default editor
ENV EDITOR="nano"

# This variable is setted to use binaries from our virtual environment
ENV PATH="/home/appwasdi/venv/bin:${PATH}"


# RUN generic to create user, group, directories, etc
RUN groupadd --gid 2042 --non-unique appwasdi && \
    useradd --create-home --home-dir /home/appwasdi --uid 2042 --gid 2042 --non-unique --shell /bin/bash appwasdi && \
    mkdir --parents \
        /home/appwasdi/.ssh \
        /opt/wasdi && \
    chown appwasdi:appwasdi \
        /home/appwasdi/.ssh && \
    chown appwasdi:appwasdi \
        /opt/wasdi && \
    chmod 700 \
        /home/appwasdi/.ssh && \
    chmod 0755 \
        /opt/wasdi


# RUN generic to update the system and install common packages
RUN echo "APT::Install-Suggests \"0\";" > /etc/apt/apt.conf.d/99local && \
    echo "APT::Install-Recommends \"0\";" >> /etc/apt/apt.conf.d/99local && \
    echo "APT::Get::Assume-Yes \"true\";" >> /etc/apt/apt.conf.d/99local && \
    echo "APT::Keep-Downloaded-Packages \"false\";" >> /etc/apt/apt.conf.d/99local && \
    echo "Dpkg::Options::--force-confold;" >> /etc/apt/apt.conf.d/99local && \
    echo "deb http://archive.ubuntu.com/ubuntu oracular main" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get install \
        ca-certificates && \
    echo "deb https://mirror.ubuntu.ikoula.com/ oracular main restricted universe multiverse"          > /etc/apt/sources.list && \
    echo "deb https://mirror.ubuntu.ikoula.com/ oracular-updates main restricted universe multiverse"  >> /etc/apt/sources.list && \
    echo "deb https://mirror.ubuntu.ikoula.com/ oracular-security main restricted universe multiverse" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get dist-upgrade && \
    apt-get install \
        curl \
        nano \
        gdal-bin \
        tar \
        unzip \
        dos2unix \
        wget && \
    sed --in-place "s/# set tabsize.*/set tabsize 4/g" /etc/nanorc && \
    sed --in-place "s/# set tabstospaces/set tabstospaces/g" /etc/nanorc && \
    rm --recursive --force /var/lib/apt/lists/* && \
    find / -xdev -type d -name __pycache__ -exec rm --recursive --force {} +


# RUN specific to install Python
RUN apt-get update && \
    apt-get install \
        python3 \
        python3-venv && \
    rm --recursive --force /var/lib/apt/lists/* && \
    find / -xdev -type d -name __pycache__ -exec rm --recursive --force {} +


# From now, all commands executed are executed with the user 'appwasdi'
USER appwasdi


# RUN specific to install pip libs
RUN python3 -m venv --system-site-packages /home/appwasdi/venv && \
    pip3 install --no-cache-dir --no-compile \
        wasdi numpy geoserver-rest mailjet-rest pandas pymongo shapely && \
    find /home/appwasdi -xdev -type d -name __pycache__ -exec rm --recursive --force {} +


# Set the workdir
WORKDIR /home/appwasdi

#COPY  --chown=appwasdi:appwasdi pip.txt /home/appwasdi/
#COPY  --chown=appwasdi:appwasdi installUserPackage.sh /home/appwasdi/

# Install packages only if /home/appwasdi/pip.txt exists
#RUN bash /home/appwasdi/installUserPackage.sh \
#        --application-directory /home/appwasdi \
#        --home-directory /home/appwasdi \
#        --package-pip

ENV PYTHONPATH="${PYTHONPATH}:/home/appwasdi"

# Copy data
COPY --chown=appwasdi:appwasdi ./RISE_deamon/src/ /home/appwasdi/src

# Start the container
CMD [ \
    "/home/appwasdi/venv/bin/python3", \
    "/home/appwasdi/src/rise/RiseDeamon.py" ]
