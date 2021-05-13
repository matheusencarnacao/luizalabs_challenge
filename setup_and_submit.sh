set -e
set -x

python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
deactivate

pushd venv/
zip -rq ../venv.zip *
popd

zip -rq app.zip app/

spark-submit \
    --name "Luiza labs challenge" \
    --archives "venv.zip#venv" \
    --py-files "app.zip" \
    app/main.py
