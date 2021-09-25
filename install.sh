#!/usr/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

conda create --name jax-ntk python=3.8 -y
source /home/weidagogo/anaconda3/bin/activate
conda activate jax-ntk
wait
echo -e "${GREEN}Created jax-ntk Env${NC}"

pip install --upgrade pip
pip install -U --upgrade "jax[cuda101]" -f https://storage.googleapis.com/jax-releases/jax_releases.html
wait
echo -e "${GREEN}Installed jax${NC}"

pip install --upgrade pip
pip install -U neural-tangents
wait
echo -e "${GREEN}Installed neural-tangents${NC}"

pip install --upgrade pip
pip install -U --upgrade tensorflow tensorflow-datasets more-itertools
wait
echo -e "${GREEN}Installed tensorflow tensorflow-datasets more-itertools${NC}"

pip install --upgrade pip
pip install -U matplotlib opencv-python notebook
wait
echo -e "${GREEN}Installed matplotlib opencv-python notebook${NC}"
