from igramscraper.instagram import Instagram
import subprocess, logging
import pandas as pd
import json
import pathlib as pl

sheet_name = '2'
start = 100
end = 200

crawl_list = pd.read_excel('save_list.xlsx',sheet_name=sheet_name)
crawl_list = crawl_list['crawl_list_{}'.format(sheet_name)].tolist()
ig = Instagram()

# authentication supported

def getfiles(folder_path, filename_extensions=['json'], parent_folder=0):
    '''
    Get all the pathlib paths inside the "RawData" folder

    :param folder                   :   specific folder inside RawData , get all if none specified
    :param filename_extension       :   file type you want to search
    :param parent_folder            :   number of layers you want to go up
    :return                         :   all the pathlib paths
    '''

    cwd = pl.Path.cwd()
    for i in range(parent_folder):
        cwd = cwd.parent

    if folder_path == None:
        full_path = cwd.joinpath('extra_crawl_with_location_id')
    else:
        full_path = cwd.joinpath('extra_crawl_with_location_id', folder_path)
    inputFiles = {}
    for filename_extension in filename_extensions:
        filename_extension = '*.' + filename_extension
        entries = pl.Path(full_path).rglob(filename_extension)
        for entry in entries:
            inputFiles.update({entry.name: [int(entry.lstat().st_mtime), entry]})

    return inputFiles

def get_nwd_infos(ig,user_id):

    temp_profile_dict={}
    account = ig.get_account_by_id(user_id)

    if account.connected_fb_page is not None :
        medium = account.connected_fb_page
    else:
        medium = account.external_url

    temp_profile_dict.update({'name': account.username,
                              'entity_id':user_id,
                              'profile_pic': account.profile_pic_url_hd,
                              'url': 'PH',
                              'medium': medium,
                              'fanCount': account.followed_by_count,
                              'post_count': account.media_count,
                              'crawled_time': int(account.modified)})

    return temp_profile_dict

def user_data_crawler(list_input):
    users_dict = {}
    successful_crawled = []
    not_successful_crawled = []
    for idx,user in enumerate(list_input):
        if (idx+1)%50 == 0:
            print('{} users crawled'.format(idx+1))
        try:
            temp_dict = get_nwd_infos(ig,user)
            users_dict.update({user:temp_dict})
            successful_crawled.append(user)
        except:
            print('{} has some crawling problems'.format(user))
            not_successful_crawled.append(user)

    return users_dict,successful_crawled,not_successful_crawled

# users_dict_extra,temp,not_crawled = user_data_crawler(remaining_id)
# json.dump(users_dict, open('users_dict.json', 'w'))
users_dict = json.load(open('users_dict.json', 'r'))
users_name_list = [v['name'] for k,v in users_dict.items()]
users_name_id = {v['name']:k for k,v in users_dict.items()}
all_existing_json_paths = getfiles(None)
all_existing_jsons = [k.replace('.json','') for k in list(all_existing_json_paths.keys())]

remaining_name = [i for i in users_name_id.keys() if i not in all_existing_jsons]

def metadata_crawler(crawl_source, folder_path, crawldatetime):
    folder_path = pl.Path.cwd().joinpath(folder_path)
    cmd = 'instagram-scraper {} --media-types none --destination {} --include-location --media-metadata --latest --time {} --interactive --profile-metadata'.format(
        crawl_source, folder_path, int(crawldatetime))
    no_of_error = 0
    error_msgs = []
    try:
        # subprocess.run(cmd, shell=True)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            std_line = p.stdout.readline()
            if not std_line:
                break
            line_str = str(std_line.rstrip(), 'utf-8')
            if len(line_str) > 0:
                for line in line_str.split('\r'):
                    print(line)
                    # logging.info(line)

    except Exception as e:
        no_of_error += 1
        error_msgs.append(e)
        # logging.error(e)
        print.error(e)

successful_crawled = []
not_successful_crawled = []

crawl_list_for_remaining = [v['name'] for k,v in users_dict.items() if v['name'] not in all_existing_jsons]

for idx,name in enumerate(crawl_list_for_remaining[start:end]):

    if (idx+1)%10 == 0:
        print('{} users crawled'.format(idx+1))
    try:
        if name not in successful_crawled:
            metadata_crawler(name, 'extra_crawl_with_location_id', 1577808000)
            successful_crawled.append(name)
    except:
        print('{} has some crawling problems'.format(name))
        not_successful_crawled.append(name)

'''
crawl_list_partitioned = [crawl_list[i:i + 1000] for i in range(0, len(crawl_list), 1000)]

writer=pd.ExcelWriter('save_list.xlsx')
for idx, i in enumerate(crawl_list_partitioned):
    temp_df = pd.Series(i)
    temp_df.name = 'crawl_list_{}'.format(idx)
    temp_df.to_excel(writer, sheet_name=str(idx) ,index=False)
writer.save()
writer.close()
'''