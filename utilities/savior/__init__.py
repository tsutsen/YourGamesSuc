import pickle
import os


def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


def save(variable, path):
    try:
        with open(path+'.pkl', 'wb') as handle:
            pickle.dump(variable, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except:
        path = '/'.join(path.split('/')[:-1]) + '/'
        print(f'creating {path} folder')
        create_folder(path)
        with open(path+'.pkl', 'wb') as handle:
            pickle.dump(variable, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load(path):
    with open(path, 'rb') as handle:
        variable = pickle.load(handle)
    return variable


def save_txt(string, path, append=False):
    if append:
        mode = 'a'
    else:
        mode = 'w'
    with open(path, mode) as handle:
        handle.write(string)


def load_txt(path, to_list=True):
    with open(path, 'rb') as handle:
        lines = handle.readlines()

    lines = [x.decode("utf-8") for x in lines]
    if to_list:
        return lines
    else:
        return ''.join(lines).strip('\n')
