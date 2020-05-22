import paho.mqtt.client as paho
import os
import socket
import ssl
import json
import psycopg2

 
def on_connect(client, userdata, flags, rc):
    print("Connection returned result: " + str(rc) )
    client.subscribe("#" , 1 )

def bulkInsert(records):
    try:
        connection = psycopg2.connect(user = "cwviet",
                                  	  password = "viet8660",
                                      host = "maindb.cexrfznwa2iv.ap-northeast-2.rds.amazonaws.com",
                                      port = "5432",
                                      database = "vn-gems")
        cursor = connection.cursor()
        sql_insert_query = """ INSERT INTO WaterSensors (timeReceive, EC, PH, D_O, RTD) 
                           VALUES (%s,%s,%s,%s,%s) """
        # executemany() to insert multiple rows rows
        result = cursor.executemany(sql_insert_query, records)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Failed inserting record into GEMS3512 table {}".format(error))
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def on_message(client, userdata, msg):                      # Func for receiving msgs
    print("topic: "+msg.topic)
    print("payload: "+str(msg.payload))
    data = msg.payload
    dic = json.loads(data)
    #print(dic['EC'])
    dataSendSQL = [(dic['Time'],dic['EC'],dic['PH'],dic['DO'],dic['RTD'])]
    print(dataSendSQL)
    bulkInsert(dataSendSQL)

#def on_log(client, userdata, level, msg):
#    print(msg.topic+" "+str(msg.payload))
 
mqttc = paho.Client()                                       # mqttc object
mqttc.on_connect = on_connect                               # assign on_connect func
mqttc.on_message = on_message                               # assign on_message func
#mqttc.on_log = on_log

#### Change following parameters ####  
awshost = "a1uonjq7219e85-ats.iot.us-west-2.amazonaws.com"      # Endpoint
awsport = 8883                                              # Port no.   
clientId = "WaterSensors"                                     # Thing_Name
thingName = "WaterSensors"                                    # Thing_Name
caPath = "AmazonRootCA1.pem"                                      # Root_CA_Certificate_Name
certPath = "eb645b1b58-certificate.pem.crt"                            # <Thing_Name>.cert.pem
keyPath = "eb645b1b58-private.pem.key"                          # <Thing_Name>.private.key
 
mqttc.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)      # pass parameters
 
mqttc.connect(awshost, awsport, keepalive=60)               # connect to aws server
 
mqttc.loop_forever()                                        # Start receiving in loop