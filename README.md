Containerize Tree Mortality with Sciunit
=====================

explain in detail: [Containerize Tree Mortality with Sciunit](https://yuszqybkpdvc.larksuite.com/wiki/ONx8wQEnTikEJVkEWGFuVrPosSf?from=from_copylink)


I created a Dockerfile to simplify the use of the workflow. You can find it here: [Dockerfile for Tree-Mortality](./docker/dockerfile).
Here’s a quick demonstration of how to use it:
1. Download the required data.
```
wget https://depauledu-my.sharepoint.com/:u:/g/personal/xchu3_depaul_edu/EVkiqIrakgxBi1xsf4VU2EAB-3Ibpt0f8Jenqltd3KwEjA?download=1 -O data.zip
unzip data.zip
```
2. Build and run the Docker container to execute the workflow.
```
docker build -t tree-mortality .
docker run -v ./:/shared -it tree-mortality
```
3. sequential execution
```
sciunit create tree-mortality  && export TZ='America/Chicago'
chmod +x *.sh
sciunit exec ./time.sh
```
4. parallel computing
Thank you for your patience. The code for parallel computing is still being refined and has not been released yet. We will send you an email as soon as it becomes available.

## Contributors
 - Gary Doran
 - Antonio Ferraz
 - Sudip Chakraborty
 - Peter Kalmus
 - Seungwon Lee

## About
[EcoPro](https://nasa-ecopro.org/) is a NASA-funded project to build ecological projection models to better understand how climate change will effect ecosystems. This study specifically looks at understanding the relationship between climate change and Sierra Nevada tree mortality.

## Setup
It is recommended to use the `environment.yml` file for creating an environment with `conda` for running the code.

<hr />
Copyright 2024, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged. Any commercial use must be negotiated with the Office of Technology Transfer at the California Institute of Technology.
