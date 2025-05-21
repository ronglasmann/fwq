
#NET_NAME=fwq
set -e

#. dev-cycle-env.sh

python -m pip config unset global.index-url || true

python -m pip install --upgrade pip
python -m pip install --upgrade build


#fwq --action stop --name streamlit-fwq-broker || true
#docker network rm $NET_NAME || true

# set up a network for the broker, app, and workers
#docker network create --driver bridge $NET_NAME

# install fwq and start the broker
python -m pip install  -e .
#fwq --host_volume ./data --max_message_size 262144 --net_name $NET_NAME --action start

# start worker
#docker run --name fwq-worker --rm \
#           --env RUNTIME_ENV=Dev \
#           --env AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
#           --env AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
#           --network $NET_NAME \
#           -v data:/data \
#           -d "streamlit-$APP" \
#           fwq --action do_jobs --for StreamlitAA


#  docker logs -f "$APP"
docker ps

# run unit tests
python -m pip install pytest

pytest -s
