FROM public.ecr.aws/lambda/python:3.10

# install the few Python dependencies
RUN pip3 install --upgrade pip \
    && yum install gcc gcc-c++ -y \
    && pip3 install pyiceberg==0.6.0 pynessie==0.66.0 --target "${LAMBDA_TASK_ROOT}"

ENV HOME=/home/aws

COPY app.py ${LAMBDA_TASK_ROOT}

CMD [ "app.lambda_handler" ]
