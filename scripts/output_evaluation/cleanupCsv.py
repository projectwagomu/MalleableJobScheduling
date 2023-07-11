# ---------------------------------------------------------------------
# Copyright (c) 2023 Wagomu project.
#
# This program and the accompanying materials are made available to you under
# the terms of the Eclipse Public License 1.0 which accompanies this
# distribution,
# and is available at https://www.eclipse.org/legal/epl-v20.html
#
# SPDX-License-Identifier: EPL-2.0
# ---------------------------------------------------------------------
import sys
import csv
import os
from enum import Enum


class FileUsage(Enum):
    ignore = 0
    keep_changes = 1
    keep_changes_and_last_row = 2


FILES_TO_CLEANUP = {"cpu_utilization.csv": FileUsage.keep_changes_and_last_row,
                    "gpu_utilization.csv": FileUsage.keep_changes_and_last_row,
                    "job_statistics.csv": FileUsage.keep_changes,
                    "network_activity.csv": FileUsage.keep_changes,
                    "node_utilization.csv": FileUsage.keep_changes,
                    "pfs_utilization.csv": FileUsage.keep_changes}

CLEANUP_SUFFIX = "_cleaned.csv"
FILE_SUFFIX = ".csv"


def cleanup_files(path):
    for file_path in os.listdir(path):
        if file_path not in FILES_TO_CLEANUP or FILES_TO_CLEANUP[file_path] is FileUsage.ignore:
            continue
        remove_duplicate_rows(path, file_path, FILES_TO_CLEANUP[file_path])


# remove rows that are equal to the row above, the equal check ignores the first column
def remove_duplicate_rows(path: str, file_path: str, file_usage=FileUsage.keep_changes):
    def has_row_changed(old_row, new_row):
        if old_row is None:
            return True
        return any(old_row[i] != new_row[i] for i in range(1, min(len(old_row), len(new_row))))

    out_path = file_path.replace(FILE_SUFFIX, CLEANUP_SUFFIX)
    with open(path + "/" + file_path) as in_file:
        with open(path + "/" + out_path, "w") as out_file:
            data_reader = csv.reader(in_file)
            data_writer = csv.writer(out_file)
            prev_line = None
            for line in data_reader:
                if has_row_changed(prev_line, line):
                    data_writer.writerow(line)
                prev_line = line.copy()
            if (file_usage is FileUsage.keep_changes_and_last_row):
                data_writer.writerow(["Last Row:"])
                data_writer.writerow(prev_line)


# include directory of .csv files as argument, default=current work directory
if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError("Unknown Argument Size")
    cleanup_files(sys.argv[1])
