FROM python
USER root
COPY . /home/
WORKDIR /home/
RUN pip3 install -r /home/requirements.txt -i https://pypi.douban.com/simple --trusted-host=pypi.douban.com
CMD ["python3","main.py"]

