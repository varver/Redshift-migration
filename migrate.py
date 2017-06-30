############ REQUIREMENTS ####################
# sudo apt-get install python-pip
# sudo apt-get install libpq-dev
# sudo pip install psycopg2
# sudo pip install sqlalchemy
# sudo pip install sqlalchemy-redshift
##############################################

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


############## Provide All Tables with Schema ####################
BACKUP_TABLES = [
  "demo.table1",
  "demo.table2",
  "demo.table3"
]
##################################################################

######################### Source DB ##############################
SOURCE_DATABASE = "db1"
SOURCE_USER = "root"
SOURCE_PASSWORD = "root"
SOURCE_HOST = "trips3m-redshift.source.us-east-1.redshift.amazonaws.com"
SOURCE_PORT = "5439"
###################################################################

######################### Destination DB ##########################
DESTINATION_DATABASE = "db2"
DESTINATION_USER = "root"
DESTINATION_PASSWORD = "root"
DESTINATION_HOST = "trips3m-redshift.destination.us-east-1.redshift.amazonaws.com"
DESTINATION_PORT = "5439"
####################################################################

############## S3 ACCESS DETAILS  ##################################
S3_BUCKET = "data-redshift"
S3_ACCESS_ID = "AKIAJHJHUBBNNGGKGHHGHGHGHGHHG"
S3_ACCESS_KEY = "hjjhdfdshfydghjfdjkhdfjhgdfgjhdfjghddjkfghddfhj"
####################################################################

CURRENT_UNLOAD_COUNT = None

def makeconnection(USER,PASSWORD,HOST,PORT,DATABASE):
  connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % (USER,PASSWORD,HOST,str(PORT),DATABASE)
  #print connection_string
  engine = sa.create_engine(connection_string)
  session = sessionmaker()
  session.configure(bind=engine)
  s = session()
  return s 

def extractTableCreateStatement(ff):
  d = []
  for x in ff :
    k = " ".join(x)
    d.append(k)
  v = " ".join(d)
  g = v.replace("\t","")
  g = g.split(";")
  g.pop(0)   # removed drop table statement
  g = "".join(g)
  return g

def CreateTable(schema,table):
    s = ConnectToSource()
    query = "select ddl from admin.v_generate_tbl_ddl where schemaname = '%s' and tablename = '%s'" % (schema,table)
    rr = s.execute(query)
    all_results =  rr.fetchall()
    #print all_results
    create_statement = extractTableCreateStatement(all_results)
    print create_statement 
    s.close()
    s = ConnectToDestination()
    k = s.execute(create_statement)
    s.commit()
    s.close()
    return True

def ConnectToSource():
  s = makeconnection(SOURCE_USER,SOURCE_PASSWORD,SOURCE_HOST,SOURCE_PORT,SOURCE_DATABASE)
  return s 

def ConnectToDestination():
  s = makeconnection(DESTINATION_USER,DESTINATION_PASSWORD,DESTINATION_HOST,DESTINATION_PORT,DESTINATION_DATABASE)
  return s 

def PrepareSelectQuery(table_name):
  tmp = table_name.split(".")
  query = """SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position""" % (tmp[0],tmp[1])
  s = ConnectToSource()
  rr = s.execute(query)
  all_results =  rr.fetchall()
  dd = ""
  for x in all_results :
      dd += '"' + x[0] + '",'
  dd = dd[0:-1]
  final_query = "select %s from %s" % (dd,table_name)
  s.close()
  return final_query


def UnloadTable(table_name):
  print "Start Unload for table ", table_name
  table_name = table_name.strip()
  tmp = table_name.split(".")
  select = PrepareSelectQuery(table_name)
  s3_path = "s3://%s/redshift_backup/%s/%s" % (S3_BUCKET,tmp[0], tmp[1])
  query = """ unload ('%s') to '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' MANIFEST  DELIMITER AS '|' NULL AS 'null_string' ESCAPE ALLOWOVERWRITE ADDQUOTES""" % (select,s3_path,S3_ACCESS_ID,S3_ACCESS_KEY)
  print query
  try :
    session = ConnectToSource()
    rr = session.execute(query)

    # get the count of documents
    query = "select count(*) from %s" % table_name
    rr = session.execute(query)
    all_results =  rr.fetchone()
    global CURRENT_UNLOAD_COUNT
    CURRENT_UNLOAD_COUNT = int(all_results[0])
    session.close()
    print "UNLOAD completed for %s" % table_name
  except Exception as e:
    print "Unable to UNLOAD TABLE %s" % table_name
    print e 
    exit()


def CopyTable(table_name):
  table_name = table_name.strip()
  print "Start Copy for table ", table_name
  tmp = table_name.split(".")
  s3_path = "s3://%s/redshift_backup/%s/%smanifest" % (S3_BUCKET,tmp[0], tmp[1])
  query = """copy %s from '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' MANIFEST DELIMITER AS '|' NULL AS 'null_string' ESCAPE ACCEPTANYDATE EXPLICIT_IDS REMOVEQUOTES TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'""" % (table_name,s3_path,S3_ACCESS_ID,S3_ACCESS_KEY)  
  print query
  try :
    session = ConnectToDestination()
    rr = session.execute(query)
    session.commit()

    # get the count of documents
    query = "select count(*) from %s" % table_name
    rr = session.execute(query)
    all_results =  rr.fetchone()
    global CURRENT_UNLOAD_COUNT
    CURRENT_COPY_COUNT = int(all_results[0])
    session.close()
    if CURRENT_COPY_COUNT == CURRENT_UNLOAD_COUNT : 
      print "COPY completed for table %s , document expoerted = %s" % (table_name,str(CURRENT_COPY_COUNT)) 
    else : 
      print "COPY failed for table %s , Count mismatch , source = %s and destination = %s" % (table_name,str(CURRENT_UNLOAD_COUNT), str(CURRENT_COPY_COUNT))
      print "exit now ............  Terminated script"
      exit()
  except Exception as e:
    no_schema_found = """schema "%s" does not exist""" % tmp[0]
    tbstr = "table %s" % tmp[1]
    session.rollback()
    if no_schema_found in str(e):
        try : 
            print "Creating schema %s" % tmp[0]
            schema_query = """create schema if not exists %s authorization %s """ % (tmp[0],DESTINATION_USER)
            sch = session.execute(schema_query)
            session.commit()
            # create table as well 
            try : 
                CreateTable(tmp[0],tmp[1])
                print "table %s is created successfully" % table_name
                CopyTable(table_name)
                print "schema %s created successfully." % tmp[0]
                return
            except Exception as p : 
                print "Error in creating table %s : " % table_name,  p 
                exit()
            
        except Exception as e : 
            print "Unable to COPY TABLE %s" % table_name
            exit()
    
    # if schems is there but table not found     
    elif tbstr in str(e) : 
      try : 
          CreateTable(tmp[0],tmp[1])
          print "table %s is created successfully" % table_name
          CopyTable(table_name)
          print "schema %s created successfully." % tmp[0]
          return
      except Exception as p : 
          print "Error in creating table %s : " % table_name,  p 
          exit()
    print "Unable to COPY TABLE %s " % table_name , e 
    exit()



def Main():
  for table in BACKUP_TABLES : 
    print "-- Start for Table ", table
    UnloadTable(table)
    CopyTable(table)
    print "-- End for Table \n\n", table


Main()
