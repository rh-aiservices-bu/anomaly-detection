FROM registry.access.redhat.com/ubi8/python-39:latest

COPY requirements.txt ./requirements.txt

RUN pip install cython>=0.22
RUN pip install numpy
RUN pip install wheel
RUN pip install pystan==2.19.1.1
RUN pip install -r requirements.txt

COPY app.py ./app.py
COPY prediction.py ./prediction.py
COPY model.pkl ./model.pkl

CMD ["python3", "app.py", "8080"]
