from kafka import KafkaConsumer
# -- coding: utf-8 --
import csv
import re
from pathlib import Path

# Script to save Continous Producer data using consumer
def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

def main():
    '''
    Consumer News from producer
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer('guardian2')
    root = str(get_project_root())
    print("Project Root Path : " + root)
    file_name_with_path = root + "/data/streaming_news_data.csv"
    with open(file_name_with_path, mode='w+') as csv_file:

        for msg in consumer:
            output = msg.value.decode('UTF-8')
            index = str(output).split('||')[0]
            index = ''.join([n for n in index if n.isdigit()])

            msgBody = (output).split('||')[1].encode("utf-8")

            msgheading = re.findall("^\w+\W+[\w+\s?\d?'$'?]+", str(msgBody))
            if not (msgheading):
                msgheading = ["NA"]
            msgheading = msgheading[0]
            fieldnames = ['index', 'heading' ,'msg_body']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writerow({'index': index, 'heading' :msgheading , 'msg_body': msgBody})
            print(msgheading)
            print(index)
            print(msgBody)
            #print(msg)
            #print(output)
    writer.close()

if __name__=='__main__':
    main()