#!/usr/bin/env python

import sys
import os
from datetime import datetime
import tempfile
from bz2 import BZ2File
from lxml import etree as et
import psycopg2
from tqdm import tqdm

CON = psycopg2.connect(
    dbname="osm",
    user="vanexel",
    host="localhost")

# Number of changeset
DEFAULT_BULK_COPY_SIZE = 1000000

class ChangesetGroup:

    _changesets = []

    def __init__(self, capacity=DEFAULT_BULK_COPY_SIZE):
        self._capacity = capacity

    def append(self, changeset):
        self._changesets.append(changeset)
        if len(self._changesets) == self._capacity:
            self.commit()
            self._changesets.clear()

    def commit(self):
        cur = CON.cursor()
        with tempfile.TemporaryFile() as file_handle:
            pbar = tqdm(self._changesets)
            for changeset in pbar:
                pbar.set_description("Writing...")
                file_handle.write(changeset.as_tsv + b"\n")
            file_handle.seek(0)
            cur.copy_from(file_handle, 'changesets')
            CON.commit()


class Changeset:

    def __init__(self, attribs):
        self._id = attribs.get("id")
        self._created_at = attribs.get("created_at")
        self._closed_at = attribs.get("closed_at")
        self._open = attribs.get("open")
        self._user = attribs.get("user")
        self._uid = attribs.get("uid")
        self._min_lat = attribs.get("min_lat")
        self._max_lat = attribs.get("max_lat")
        self._min_lon = attribs.get("min_lon")
        self._max_lon = attribs.get("max_lon")
        self._comments_count = attribs.get("comments_count")
        self._num_changes = attribs.get("num_changes")

    @classmethod
    def from_xml(cls, elem):
        changeset = cls(elem.attrib)
        return changeset

    @property
    def id(self):
        return int(self._id)

    @property
    def created_at(self):
        return datetime.strptime(
            self._created_at, "%Y-%m-%dT%H:%M:%SZ")

    @property
    def closed_at(self):
        return datetime.strptime(
            self._closed_at, "%Y-%m-%dT%H:%M:%SZ")

    @property
    def open(self):
        return self._open == 'true'

    @property
    def user(self):
        return self._user

    @property
    def uid(self):
        if self._uid:
            return int(self._uid)
        return -1

    @property
    def min_lat(self):
        if self._min_lat:
            return float(self._min_lat)
        return 0.0

    @property
    def max_lat(self):
        if self._max_lat:
            return float(self._max_lat)
        return 0.0

    @property
    def min_lon(self):
        if self._min_lon:
            return float(self._min_lon)
        return 0.0

    @property
    def max_lon(self):
        if self._max_lon:
            return float(self._max_lon)
        return 0.0

    @property
    def comments_count(self):
        return int(self._comments_count)

    @property
    def num_changes(self):
        return int(self._num_changes)

    @property
    def as_insert(self):
        return """INSERT INTO changesets (
        "id",
        "created_at",
        "closed_at",
        "open",
        "user",
        "uid",
        "min_lat",
        "max_lat", 
        "min_lon",
        "max_lon", 
        "comments_count", 
        "num_changes") VALUES (
        {id}, 
        '{created_at}',
        '{closed_at}',
        {open},
        '{user}',
        {uid},
        {min_lat}, 
        {max_lat},
        {min_lon},
        {max_lon},
        {comments_count},
        {num_changes})""".format(
            id=self.id,
            created_at=self.created_at,
            closed_at=self.closed_at,
            open=self.open,
            user=self.user,
            uid=self.uid,
            min_lat=self.min_lat,
            max_lat=self.max_lat,
            min_lon=self.min_lon,
            max_lon=self._max_lon,
            comments_count=self.comments_count,
            num_changes=self.num_changes)

    @property
    def as_tsv(self):
        # return '\t'.join(map(str,self.__dict__.values())).encode("UTF-8")
        return '\t'.join(
            str(val) if val else '\\N' for val in self.__dict__.values()).encode("UTF-8")

    def __str__(self):
        return "OSM Changeset {}".format(self.id)


def usage():
    print("Usage: parse_changesets changeset_xml.bz2")
    sys.exit()

def main():
    args = sys.argv
    if len(args) != 2:
        usage()
    changesets_file = args[1]
    changesets = ChangesetGroup()
    if not os.path.exists(changesets_file):
        usage()
    with BZ2File(changesets_file) as file_handle:
        parser = et.iterparse(file_handle, events=('end',))
        pbar = tqdm(parser)
        for events, elem in pbar:
            pbar.set_description("Reading...")
            if elem.tag == "changeset":
                changeset = Changeset.from_xml(elem)
                changesets.append(changeset)

if __name__ == "__main__":
    main()
    CON.close()
