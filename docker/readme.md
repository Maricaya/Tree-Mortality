# Pre-build requirements:
    wget https://depauledu-my.sharepoint.com/:u:/g/personal/xchu3_depaul_edu/EVkiqIrakgxBi1xsf4VU2EAB-3Ibpt0f8Jenqltd3KwEjA?download=1 -O data.zip
    unzip data.zip

# Build:
    docker build -t tree-mortality .


# Usage:
    docker run -v ./:/shared -it tree-mortality

    sciunit create tree-mortality  && export TZ='America/Chicago'
    chmod +x *.sh
    sciunit exec ./scripts/convert_bcm.sh # Run based on dag.png