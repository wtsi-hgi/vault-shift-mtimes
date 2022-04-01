#!/usr/bin/env python3 

import os
from pathlib import Path

# CLI library https://github.com/tiangolo/typer :
import typer

# import functions to deal with dates and time:
import datetime
from dateutil.relativedelta import relativedelta

# import functions from several functional-programming-style libraries:
from toolz import compose_left, curry, pipe, thread_last
from toolz.curried import map, filter
from more_itertools import consume, side_effect

# definitions of 'pure' functions below:
# 'pure' in the sense that they don't have side effects (like logging or modifying files)
def yield_files(dir_):
    "Yield all files in a directory, ignoring symbolic links"
    yield from thread_last(dir_, 
                       Path,
                       (lambda v: v.iterdir()),
                       (filter, lambda v: v.is_file() and not v.is_symlink()))

def yield_directories(dir_):
    "Yield all directories in a directory, ignoring symbolic links"
    yield from thread_last(dir_, 
                       Path,
                       (lambda v: v.iterdir()),
                       (filter, lambda v: v.is_dir() and not v.is_symlink()))

def yield_files_recursive(dir_):
    """
    Recursively yield all files in all subdirectories of an input directory,
    ignoring symbolic links
    """
    yield from yield_files(dir_)
    for directory in yield_directories(dir_):
        yield from yield_files_recursive(directory)

def epochs_to_datetime(epochtime_):
    """
    Convert epochs float to datetime object.
    The epoch time is also known as POSIX time which will indicate the number of seconds passed 
    from January 1, 1970,00:00:00 (UTC) in most windows and Unix systems.
    """
    return datetime.datetime.fromtimestamp(epochtime_)

def datetime_to_epochs(datetime_):
    "convert datetime object to epochs float"
    return datetime_.timestamp()

def datetime_str_to_datetime(date_str_, format_ = '%Y-%m-%d'):
    "convert datetime string in specific input format to a datetime object"
    return datetime.datetime.strptime(date_str_, format_)

def datetime_str_to_epochs(date_str_, format_ = '%Y-%m-%d'):
    "convert datetime string in specific input format to a epoch time string"
    return datetime_to_epochs(datetime_str_to_datetime(date_str_, format_))

def datetime_to_datetime_str(datetime_, format_ = '%Y-%m-%d'):
    "convert datetime object to a datetime string in specific string format"
    return datetime_.strftime(format_)

def epochs_to_datetime_str(epochs_, format_ = '%Y-%m-%d'):
    "convert datetime object to epochs float"
    return datetime_to_datetime_str(epochs_to_datetime(epochs_), format_ = format_)

def add_months_days_to_datetime(datetime_, n_months_, n_days_):
    "shift forward a datetime object by adding a certain amount of months and days"
    return datetime_ + relativedelta(months=n_months_, days=n_days_)

def file_to_dict(file_):
    """
    from input file, extract mtime, ctime in different formats, 
    and get newest (max) datetime of both.
    Store info into a dictionary (to be processed by later functions)
    """
    stat_ = os.stat(Path(file_))
    max_ = max(stat_.st_mtime, stat_.st_ctime)
    return {
      "mtime_epoch":     stat_.st_mtime,
      "ctime_epoch":     stat_.st_ctime,
      "max_mt_ct_epoch": max_,
      "max_mt_ct_dt":    epochs_to_datetime(max_),
      "mtime_str":       epochs_to_datetime_str(stat_.st_mtime),
      "ctime_str":       epochs_to_datetime_str(stat_.st_ctime),
      "max_mt_ct_str":   epochs_to_datetime_str(max_),
      "file":            file_ }

# 'impure' functions below, as they have side effect of modfying files atime and mtimes:
def set_mtime_ctime_to_datetime(file_, datetime_):
    """
    use os.utime() to set the atime and mtime of a file 
    to a specific date which is provided as input (as a datetime object)"
    """
    epochs = datetime_to_epochs(datetime_)
    os.utime(file_.as_posix(),(epochs, epochs))
    return None

def shift_mtime(file_, datetime_to_shift, shift_add_months_, shift_add_days_):
    """
    set the atime and mtime of a file to a specific date, which is 
    input date + n months + n days (effectively shifting the a/mtime of a file forward)
    """
    new_mtime = add_months_days_to_datetime(datetime_to_shift, shift_add_months_, shift_add_days_)
    # set_mtime_ctime_to_datetime(file_, new_mtime)
    return 'file:' + file_.as_posix() + ',new_mtime:' + datetime_to_datetime_str(new_mtime)

# curry function side_effect so it can be used in pipe() function (see application below)
#   cf. 'curry()' doc at https://toolz.readthedocs.io/en/latest/curry.html
#     and 'side_effect()' doc at https://more-itertools.readthedocs.io/en/stable/api.html#more_itertools.side_effect
side_effect_curried = curry(side_effect)

# main functions composition to recursively process files (i.e. shift their mtime forward by adding n months/days):
def shift_files_mtimes_recursively(shift_add_months, shift_add_days, shift_older_than_cutoff):
    return compose_left(
        # turn input directory path string into Path object:
        Path, 
        # from that directory, get all files recursively in a generator (cf. 'yield' def):
        yield_files_recursive, 
        # for each file, create a dictionary to hold mtimes and other useful metadata:
        map(file_to_dict), 
        # use filter to keep only files that are older (as per mtime) than a certain input date:
        filter(lambda v: v['mtime_epoch'] < datetime_to_epochs(shift_older_than_cutoff)),
        # finally, for each file/dictionary, shift the mtime forward as per the input parameters:
        map(lambda v: shift_mtime(v['file'],v['max_mt_ct_dt'],shift_add_months, shift_add_days)))

# CLI entrypoint: 
#input_dir = '/lustre/scratch123/hgi/projects/ukbb_scrna/recover'
# how many months and days to add to mtimes
# only operate (shift mtimes) on files older than ..
# in datetime format %Y-%m-%d'

def main(input_dir: Path = typer.Argument(..., 
                                          exists=True, file_okay=False, dir_okay=True,
                                          writable=True, readable=True, resolve_path=True),
         shift_add_months: int = typer.Argument(..., min=0, max=1000), 
         shift_add_days: int = typer.Argument(..., min=0, max=1000),
         shift_older_than_cutoff: datetime.datetime = typer.Argument(..., formats=["%Y-%m-%d"]),
        ): 
    typer.echo(f"input dir: {input_dir}")
    typer.echo(f"input shift_add_months: {shift_add_months}")
    typer.echo(f"input shift_add_days: {shift_add_days}")
    typer.echo(f"input shift_older_than_cutoff: {shift_older_than_cutoff}")
    
    # Run the composed function on input dir, by 'consuming' the iterable of files:
    pipe(input_dir, # start pipe (like Linux bash pipes) with the input directory
         # apply the main function to recursively find all files in subdirectories and shift their mtime:
         shift_files_mtimes_recursively(shift_add_months, shift_add_days, shift_older_than_cutoff),
         # use 'side_effect' function to log the iterable of files as it is being consumed/processed
         side_effect_curried(lambda string_to_log_: print(string_to_log_), 
                         before = lambda: print("processing files..."),
                         after = lambda: print("done processing files...")),
         # use 'consume' to actually iterate over the iterable, as opposed to just return it unconsumed:
         consume)
    

if __name__ == "__main__":
    typer.run(main)
