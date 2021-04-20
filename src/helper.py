# -*- coding: utf-8 -*-

import os, sys
import ntpath

def splitFilePath(filepath):
    """ Split filepath into folderpath, filename and file extension
    
    :param filepath: Filepath
    :type filepath: str
    """
    folderpath, _ = ntpath.split(filepath)
    head, file_extension = os.path.splitext(filepath)
    folderpath, filename = ntpath.split(head)
    return folderpath, filename, file_extension

def splitFolderPath(folderpath):
    """ Split filepath into folderpath of parent folder and foldername
    
    :param folderpath: Folderpath
    :type folderpath: str
    """
    
    folderpath_parent, foldername = ntpath.split(folderpath)
    return folderpath_parent, foldername