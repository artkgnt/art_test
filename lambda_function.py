import json
import boto3
import urllib

env = 'dev'
bk =  'kgi-sanofi'

def send2dir(file_name):
    #for now always send to Inpatient_Confinement
    #but we will do matching later according to our config file 
    #and send to the right dir
    
    return 'Inpatient_Confinement'
    
def lambda_handler(event, context):
    import config

    cfg = new Config()
    print cfg.get('any')
    
    s3 = boto3.client('s3')

    arrived_file = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'])  #'csv-files/test2.csv'
    
    dir = send2dir(arrived_file)

    new_name = env + '/csv_files_raw/' + dir + '/' + arrived_file.split('/')[-1]
    print 'Copying {} to {} ...'.format(arrived_file, new_name)

    copy_source = {'Bucket': bk, 'Key': arrived_file}
    s3.copy_object(CopySource = copy_source, Bucket = bk, Key = new_name)

    return {
        "statusCode": 200,
        "body": json.dumps('in there!!!, event=' + str(event) + 'cfg->' + cfg.get('any'))
    }
