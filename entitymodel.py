# -*- coding: utf-8 -*-
# $Author: $
# $Date: $
# $Rev: $
# $URL: $
"""
Entity Model comprising: An entity that contains multiple instances of DataPoint.
"""
# Standard Modules
import datetime

# Local Modules


class DataPoint(object):
    """ A Data Point object containing the following:

    dpid: the unique identifier for the data point
    dpname: this is the unique name for the data point
    value:

    """

    def __init__(self, dpid, dpname, value):
        """
        :param dpid: the unique identifier for the data point
        :param dpname: this is the unique name for the data point
        :param value:
        """
        self.dpname = dpname
        self.dpid = dpid
        self.value = value

    @property
    def dpid(self):
        return self._dpid

    @dpid.setter
    def dpid(self, d):
        if not d:
            raise Exception("Data Point ID must be set.")
        self._dpid = d

    @property
    def dpname(self):
        return self._dpname

    @dpname.setter
    def dpname(self, d):
        if d == "":
            raise Exception("Data Point Name must be set or None.")
        self._dpname = d

    @property
    def effectivedate(self):
        return self._effectivedate

    @effectivedate.setter
    def effectivedate(self, ed):
        # Check that the input string is a date
        if ed and not isinstance(ed, datetime.datetime):
            raise TypeError("Effective Date must be a valid datetime object.")
        self._effectivedate = ed

    def __repr__(self):
        return (self.__class__.__name__ +
                ("(dpid={}, dpname={}, value={})".format(self._dpid, self._dpname, self.value)))


class ProvenancePoint(object):
    """ A Provenance Point object for a DataPoint containing the following:

    dpid: the unique identifier for the data point
    dpname: this is the unique name for the data point
    value:
    effectivedate:
    parentdatapointid:

    """

    def __init__(self, dpid, dpname, value, effectivedate, parentdatapointid):
        """
        :param dpid: the unique identifier for the data point
        :param dpname: this is the unique name for the data point
        :param value:
        :param effectivedate:
        :param parentdatapointid:
        """
        self.dpname = dpname
        self.dpid = dpid
        self.value = value
        self.effectivedate = effectivedate
        self.parentdatapointid = parentdatapointid

    @property
    def dpid(self):
        return self._dpid

    @dpid.setter
    def dpid(self, d):
        if not d:
            raise Exception("DP ID must be set.")
        self._dpid = d

    @property
    def dpname(self):
        return self._dpname

    @dpname.setter
    def dpname(self, d):
        if d == "":
            raise Exception("DP Name must be set or None.")
        self._dpname = d

    @property
    def effectivedate(self):
        return self._effectivedate

    @effectivedate.setter
    def effectivedate(self, ed):
        # Check that the input string is a date
        if ed and not isinstance(ed, datetime.datetime):
            raise TypeError("Effective Date must be a valid datetime object.")
        self._effectivedate = ed

    def __repr__(self):
        return (self.__class__.__name__ +
                ("(dpid={}, dpname={}, value={}, eff_date={}, parentdpid={})".format(
                    self._dpid, self._dpname, self.value, str(self._effectivedate), self.parentdatapointid)))


class DataPointChange(object):
    """ A Data Point Change object containing the following:

    dpid: the unique identifier for the data point


    """
    def __init__(self, dpid, changetype, changedate):
        """
        :param dpid: the unique identifier for the data point

        """
        self.dpid = dpid
        self.changetype = changetype
        self.changedate = changedate

        @property
        def changedate(self):
            return self._changedate

        @changedate.setter
        def changedate(self, ed):
            # Check that the input string is a date
            if ed and not isinstance(ed, datetime.datetime):
                raise TypeError("Last Change Date must be a valid datetime object.")
            self._changedate = changedate

    def __repr__(self):
        return (self.__class__.__name__ +
                ("(dpid={}, changetype={}, changedate={})".format(self._dpid, self.changetype, self._changedate)))


class Entity(object):
    """

    """
    def __init__(self, identifier, source, status, timestamp):
        """

        """
        self.identifier = identifier
        self.source = source
        self.status = status
        self.contenttimestamp = timestamp  # Set by the client
        self.datapoints = []

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, d):
        if not d or d == "":
            raise ValueError("Entity Identifier must be set.")
        self._identifier = d

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, src):
        if not src or src == "":
            raise ValueError("Entity Managing Source must be set.")
        self._source = src

    @property
    def contenttimestamp(self):
        return self._contenttimestamp

    @contenttimestamp.setter
    def contenttimestamp(self, ed):
        # Check that the input string is a date
        if ed and not isinstance(ed, datetime.datetime):
            raise TypeError("Content Timestamp must be a valid datetime object.")
        self._contenttimestamp = ed

    def __repr__(self):
        return (self.__class__.__name__ +
                ("(identifier={}, source={}, status={}, datapoints={})".format(
                    self._identifier, self.source, self.status, len(self.datapoints))))
