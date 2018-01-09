FROM frolvlad/alpine-python3:latest
RUN mkdir /root/.kube
COPY server.py requirements.txt /
COPY model/ /model/
COPY config /root/.kube/
RUN pip3 install -r requirements.txt
CMD python3 server.py
