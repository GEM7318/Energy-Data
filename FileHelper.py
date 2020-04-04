import os
from datetime import datetime
import random


def read_and_shuffle_hrefs():
    """
    Reads in csv of names and hrefs and returns shuffled dictionary to
    traverse.
    """
    path_to_read = os.path.join(os.getcwd(), r'urls.csv')
    urls_df = pd.read_csv(path_to_read)

    urls = urls_df.set_index('Name')['Href'].to_dict()

    names = [val for val in urls.keys()]
    random.shuffle(names)
    shuffled_urls = {k: urls[k] for k in names}

    return shuffled_urls
# urls = read_and_shuffle_hrefs()
# print(urls.keys())


def get_file_name(folder_ext: str, file_name: str) -> str:
    """
    Creates a file name with an index number based on folder extension and
    file name inputs.
    :param folder_ext: Underscore-delimited name of folder with the last
    argument being the file type
    :param file_name: Base name of file
    :return: File name including base file name, date, and file index number
    within a given day
    """
    base_path = os.path.join(os.getcwd(), folder_ext)
    current_date = str(datetime.today()).split(' ')[0]

    files = os.listdir(base_path)
    pre_existing_files = [file for file in files if
                          file_name in file and current_date in file]
    index_num = len(pre_existing_files) + 1

    file_ext = folder_ext.split('_')[-1]
    file_name = f"{current_date} ~ {file_name} ~ v{index_num}.{file_ext}"

    return file_name
# get_file_name('outputs_csv', 'Daily Total')
# get_file_name('_txt', 'Brent')


def save_raw_html(prettified_html, href_name, folder_ext='_txt'):
    """
    Accepts prettified string of raw HTML and writes out to local text file.
    :param folder_ext: Base folder name to save raw html in
    :param prettified_html: String of HTML
    :param href_name: Name of href from which the HTML was pulled (used in
    file name)
    :return: None
    """

    file_name = get_file_name(folder_ext, href_name)
    path_to_write = os.path.join(os.getcwd(), folder_ext, file_name)

    with open(path_to_write, 'w', encoding='utf-8') as f:
        f.write(prettified_html)

    print("\t<raw html written to local text file>")

    return None