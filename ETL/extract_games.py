# import berserk
import os
import requests

# token = "lip_S4reqOQMYgsnsu9tXKjt"
# session = berserk.TokenSession(token)
# client = berserk.Client(session=session)

def download_file(url,output_directory):
    try :
        # send http get to url
        response = requests.get(url)
        # check for request status
        if response.status_code == 200 :
            #get file name
            file_name = url.split("/")[-1]
            
            #path to save the  file
            file_path = os.path.join(output_directory,file_name)
            with open(file_path,'wb') as file :
                file.write(response.content)
            print(f"File downloaded to {file_path}")
        else :
            print(f"failed to download file . HTTP STATUS code :{response.status_code}")
    except Exception as e :
        print(f"An error occurred: {str(e)}")

download_file("https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst","ETL/temp_lichess_data")