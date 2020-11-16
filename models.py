from collections import OrderedDict
import datetime
from decimal import Decimal
from typing import Optional, Tuple, List

import sqlalchemy
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint, CHAR, VARCHAR, JSON, SMALLINT, INTEGER, DATE, BOOLEAN, TIMESTAMP, TIME, ARRAY
from sqlalchemy.ext.declarative import declarative_base


def _compare_time(t1, t2) -> int:
    if not (t1 and t2):
        return 0
    t1,t2 = [a.hour*3600+a.minute*60+a.second for a in (t1,t2)]
    return (Decimal(t1)-Decimal(t2))/3600

def _combine_darwin_time(working_time, darwin_time) -> datetime.datetime:
    if not working_time or not darwin_time:
        return None

    # Crossed midnight, increment ssd offset
    if _compare_time(darwin_time, working_time) < -6:
        ssd_offset = +1
    # Normal increase or decrease, nothing we really need to do here
    elif -6 <= _compare_time(darwin_time, working_time) <= +18:
        ssd_offset = 0
    # Back in time, crossed midnight (in reverse), decrement ssd offset
    elif +18 < _compare_time(darwin_time, working_time):
        ssd_offset = -1

    return datetime.datetime.combine(working_time.date(), darwin_time) + datetime.timedelta(days=ssd_offset)


Base = declarative_base()

def create_all(engine):
    Base.metadata.create_all(engine)


class SwallowDebug(Base):
    __tablename__ = "swallow_debug"
    subsystem = Column(VARCHAR(4), primary_key=True)
    subkey = Column(VARCHAR, primary_key=True)
    disambiguation = Column(VARCHAR, primary_key=True)
    updated_at = Column(TIMESTAMP, nullable=False)
    content = Column(VARCHAR, nullable=True)

    def serialise(self):
        return OrderedDict([
            ("subsystem", self.subsystem),
            ("subkey", self.subkey),
            ("disambiguation", self.disambiguation),
            ("updated_at", self.updated_at),
            ("content", self.content)
        ])


class LocalisedReference(Base):
    __tablename__ = "localised_references"
    source = Column(VARCHAR, primary_key=True)
    locale = Column(VARCHAR(5), primary_key=True)
    code_type = Column(VARCHAR, primary_key=True)
    code = Column(VARCHAR, primary_key=True)
    description = Column(VARCHAR)

class BPlanNetworkLink(Base):
    __tablename__ = "bplan_network_links"
    origin = Column(VARCHAR(7), nullable=False, primary_key=True)
    destination = Column(VARCHAR(7), nullable=False, primary_key=True)
    running_line_code = Column(VARCHAR(3), nullable=False, primary_key=True)
    running_line_desc = Column(VARCHAR(20))
    start_date = Column(DATE, nullable=True)
    end_date = Column(DATE, nullable=True)
    initial_direction = Column(CHAR, nullable=False)
    final_direction = Column(CHAR, nullable=False)
    distance = Column(INTEGER, nullable=True)
    doo_passenger = Column(BOOLEAN, nullable=False)
    doo_non_passenger = Column(BOOLEAN, nullable=False)
    retb = Column(BOOLEAN, nullable=False)
    zone = Column(VARCHAR, nullable=False)
    reversible = Column(CHAR, nullable=False)
    power = Column(CHAR, nullable=False)
    route_allowance = Column(INTEGER, nullable=False)


class BPlanPlatform(Base):
    __tablename__ = "bplan_platforms"
    tiploc = Column(VARCHAR(7), primary_key=True)
    platform = Column(VARCHAR(3), primary_key=True)
    start_date = Column(DATE, nullable=True)
    end_date = Column(DATE, nullable=True)
    length = Column(INTEGER, nullable=True)
    power = Column(CHAR, nullable=False)
    doo_passenger = Column(BOOLEAN, nullable=False)
    doo_non_passenger = Column(BOOLEAN, nullable=False)



class DarwinOperator(Base):
    __tablename__ = "darwin_operators"

    operator = Column(CHAR(2), primary_key=True, unique=True, index=True)
    operator_name = Column(VARCHAR)
    url = Column(VARCHAR)

    # TODO: best practice dictates this would actually form part of the DB itself, but that means MIGRATION!
    # TODO: and going through all that for this is probably not worth it
    # TODO: in short, save this for when the DB tables actually need major restructuring
    def category(self):
        if self.operator in ["NY", "PC", "WR", "ZM"]:
            return "C"
        elif self.operator in ["TW", "LT", "SJ"]:
            return "M"
        return "S"

    def __repr__(self):
        return "<DarwinOperator {} - {}>".format(self.operator, self.operator_name)

class DarwinReason(Base):
    __tablename__ = "darwin_reasons"
    __table_args__ = (
        UniqueConstraint("id", "type"),
    )

    id = Column(SMALLINT, primary_key=True, index=True)
    type = Column(CHAR(1), primary_key=True)
    message = Column(VARCHAR)

    def __repr__(self):
        return "<DarwinReason {}{} - {}>".format(self.id, self.type, self.message)


class DarwinLocation(Base):
    __tablename__ = "darwin_locations"
    tiploc = Column(VARCHAR(7), nullable=False, unique=True, primary_key=True, index=True)
    crs_darwin = Column(VARCHAR(3), index=True)
    crs_corpus = Column(VARCHAR(3))
    operator = Column(VARCHAR(2))
    name_short = Column(VARCHAR)
    name_full = Column(VARCHAR)
    dict_values = Column(JSON, name="dict")
    category = Column(CHAR(1))
    name_darwin = Column(VARCHAR)
    name_corpus = Column(VARCHAR)

    lines = relationship(BPlanNetworkLink, uselist=True, lazy="select", primaryjoin="""or_(
    foreign(DarwinLocation.tiploc)==BPlanNetworkLink.origin,
    and_(foreign(DarwinLocation.tiploc)==BPlanNetworkLink.destination, BPlanNetworkLink.reversible.in_(['B', 'R']))
    )""")

    platforms = relationship(BPlanPlatform, uselist=True, lazy="select", primaryjoin="foreign(DarwinLocation.tiploc)==BPlanPlatform.tiploc", order_by="asc(BPlanPlatform.platform)")


    def __repr__(self):
        return "<DarwinLocation {} - {}>".format(self.tiploc, self.name_short)

    def serialise(self, short=True):
        return OrderedDict([
            ("tiploc", self.tiploc),
            ("location_category", self.category),
            ("crs_darwin", self.crs_darwin),
            ("name_short", self.name_short),
            ("name_full", self.name_full),
        ])


class DarwinSchedule(Base):
    __tablename__ = "darwin_schedules"
    __table_args__ = (
        UniqueConstraint("uid", "ssd"),
    )
    uid = Column(VARCHAR(7), nullable=False, index=True)
    rid = Column(CHAR(15), nullable=False, primary_key=True, unique=True, index=True)
    rsid = Column(CHAR(8))
    ssd = Column(DATE, nullable=False, index=True)
    signalling_id = Column(CHAR(4), nullable=False)
    status = Column(CHAR(1), nullable=False)
    category = Column(CHAR(2), nullable=False)

    # TODO: straighten this out
    operator_id = Column(CHAR(2), ForeignKey("darwin_operators"), nullable=False, name="operator")
    operator = relationship("DarwinOperator")

    is_active = Column(BOOLEAN, nullable=False, default=False)
    is_charter = Column(BOOLEAN, nullable=False, default=False)
    is_deleted = Column(BOOLEAN, nullable=False, default=False)
    is_passenger = Column(BOOLEAN, nullable=False, default=False)

    origins = Column(ARRAY(JSON), nullable=False)
    destinations = Column(ARRAY(JSON), nullable=False)

    delay_reason = Column(JSON, default=None)
    cancel_reason = Column(JSON, default=None)

    locations = relationship("DarwinScheduleLocation", lazy="select", uselist=True, primaryjoin="foreign(DarwinSchedule.rid)==DarwinScheduleLocation.rid", order_by="DarwinScheduleLocation.index")

    origins_rel = relationship("DarwinScheduleLocation", uselist=True, lazy="joined", primaryjoin="and_(foreign(DarwinSchedule.rid)==DarwinScheduleLocation.rid, DarwinScheduleLocation.loc_type.like('%OR'))")
    destinations_rel = relationship("DarwinScheduleLocation", uselist=True, lazy="joined", primaryjoin="and_(foreign(DarwinSchedule.rid)==DarwinScheduleLocation.rid, DarwinScheduleLocation.loc_type.like('%DT'))")


    associated_to = relationship("DarwinAssociation", uselist=True, lazy="joined", primaryjoin="foreign(DarwinSchedule.rid)==DarwinAssociation.main_rid")
    associated_from = relationship("DarwinAssociation", uselist=True, lazy="joined", primaryjoin="foreign(DarwinSchedule.rid)==DarwinAssociation.assoc_rid")


    def __repr__(self):
        return "{}/{} (r. {} rs. {}) {}".format(self.ssd, self.uid, self.rid, self.rid, self.rsid, self.operator_id)


    def serialise(self, recurse):
        """For JSONification. Recurse - if set to True, will include all calling locations"""
        out = OrderedDict([
            ("uid", self.uid),
            ("ssd", self.ssd),
            ("rid", self.rid),
            ("rsid", self.rsid),
            ("signalling_id", self.signalling_id),
            ("category", self.category),
            ("operator", self.operator_id),
            ("is_active", self.is_active),
            ("is_charter", self.is_charter),
            ("is_passenger", self.is_passenger),
            ("origins", [a.serialise(False, c, limit_associations=True) for c, a in self.get_origins()]),
            ("destinations", [a.serialise(False, c, limit_associations=True) for c, a in self.get_destinations()]),
            ("delay_reason", self.delay_reason),
            ("cancel_reason", self.cancel_reason),
        ])

        if recurse:
            out["locations"] = [a.serialise(False) for a in self.locations]
        return out


    def get_origins(self) -> List[Tuple[str, "DarwinScheduleLocation"]]:
        sc_orig = self.origins_rel
        return list(zip(len(sc_orig)*["SC"], sc_orig)) + [(a.category, b) for a in self.associated_from for b in a.main_schedule.origins_rel if a.category != "NP"]

    def get_destinations(self) -> List[Tuple[str, "DarwinScheduleLocation"]]:
        sc_dest = self.destinations_rel
        return list(zip(len(sc_dest)*["SC"], sc_dest)) + [(a.category, b) for a in self.associated_to for b in a.assoc_schedule.destinations_rel if a.category != "NP"]


class DarwinScheduleLocation(Base):
    __tablename__ = "darwin_schedule_locations"
    __table_args__ = (
        UniqueConstraint("rid", "original_wt", "tiploc"),
    )

    rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, primary_key=True)
    rid_constraint = ForeignKeyConstraint(("rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    schedule = relationship("DarwinSchedule", uselist=False, lazy="joined", innerjoin=True)

    index = Column(SMALLINT, primary_key=True)
    loc_type = Column(VARCHAR(4), nullable=False, name="type")

    tiploc = Column(VARCHAR(7), ForeignKey("darwin_locations.tiploc"), nullable=False, index=True)
    location: DarwinLocation = relationship("DarwinLocation", uselist=False)

    activity = Column(VARCHAR(12), nullable=False)
    original_wt = Column(VARCHAR(18))

    pta = Column(TIMESTAMP, default=None)
    wta = Column(TIMESTAMP, default=None, index=True)
    wtp = Column(TIMESTAMP, default=None, index=True)
    ptd = Column(TIMESTAMP, default=None)
    wtd = Column(TIMESTAMP, default=None, index=True)

    cancelled = Column(BOOLEAN, nullable=False, default=False)
    rdelay = Column(SMALLINT, nullable=False, default=0)

    status: "DarwinScheduleStatus" = relationship("DarwinScheduleStatus", uselist=False, lazy="joined", primaryjoin="""and_(
        foreign(DarwinScheduleLocation.rid)==DarwinScheduleStatus.rid,
        foreign(DarwinScheduleLocation.original_wt)==DarwinScheduleStatus.original_wt,
        foreign(DarwinScheduleLocation.tiploc)==DarwinScheduleStatus.tiploc
        )""", innerjoin=True)

    associated_to = relationship("DarwinAssociation", lazy="joined", uselist=True, primaryjoin="and_(foreign(DarwinScheduleLocation.rid)==DarwinAssociation.main_rid, foreign(DarwinScheduleLocation.original_wt)==DarwinAssociation.main_original_wt)")
    associated_from = relationship("DarwinAssociation", lazy="joined", uselist=True, primaryjoin="and_(foreign(DarwinScheduleLocation.rid)==DarwinAssociation.assoc_rid, foreign(DarwinScheduleLocation.original_wt)==DarwinAssociation.assoc_original_wt)")

    def complete_times_dict(self) -> dict:
        out = OrderedDict()
        for letter, name in zip("apd", ["arrival", "pass", "departure"]):
            this_times = OrderedDict()

            wt = getattr(self, "wt%s" % letter)
            if wt:
                this_times["working"] = wt

            pt = getattr(self, "pt%s" % letter, None)
            if pt:
                this_times["public"] = pt

            st = _combine_darwin_time(wt, getattr(self.status, "t%s" % letter))
            stt = getattr(self.status, "t%s_type" % letter)
            if st and stt:
                this_times["actual"*(stt=="A") or "estimated"] = st

            out[name] = this_times

        return out

    def complete_associations(self) -> List[Tuple[bool, "DarwinAssociation"]]:
        return [(True, a) for a in self.associated_from] + [(False, a) for a in self.associated_to]

    def complete_associations_dict(self):
        complete_list = self.complete_associations()
        straightened = []
        for k, v in complete_list:
            out = OrderedDict()
            out["from"] = k
            out["category"] = v.category
            if k:
                out["assoc"] = v.main_schedule_loc.serialise(True, v.category)
                out["there"] = out["assoc"]["origins"]
            else:
                out["assoc"] = v.assoc_schedule_loc.serialise(True, v.category)
                out["there"] = out["assoc"]["destinations"]

            straightened.append(out)
        return straightened

    def __repr__(self):
        return "<DarwinScheduleLocation {}/{}/{} wta {} wtd {} s {} f - t ->".format(self.rid, self.tiploc, self.index, self.wta, self.wtd, self.status)

    def serialise(self, recurse: bool, source: str = "SC", limit_associations=False):
        """Serialises this as a dict for presumed JSON. If recurse is set, will be enveloped by schedule"""
        # TODO: enveloping is the old paradigm, but *is it a good one*? Semantically it doesn't make so much sense now
        here = OrderedDict([
            ("type", self.loc_type),
            ("source", source),
            ("activity", self.activity),
            ("cancelled", self.cancelled),
            ("length", self.status.length),
            ("times", self.complete_times_dict()),
            ("platform", OrderedDict([
                ("platform", self.status.plat),
                ("suppressed", self.status.plat_suppressed),
                ("cis_suppressed", self.status.plat_cis_suppressed),
                ("confirmed", self.status.plat_confirmed),
                ("source", self.status.plat_source)
                ])),
            ("associations", self.complete_associations_dict() if source == "SC" and not limit_associations else [])
            ])
        here.update(self.location.serialise(True))

        if not recurse:
            return here
        else:
            schedule = self.schedule.serialise(not recurse)
            schedule["here"] = here
            return schedule


class DarwinAssociation(Base):
    __tablename__ = "darwin_associations"
    __table_args__ = (
        UniqueConstraint("main_rid", "assoc_rid", "tiploc"),
    )

    category = Column(CHAR(2), nullable=False)
    tiploc = Column(VARCHAR(7), ForeignKey("darwin_locations.tiploc"), nullable=False, index=True, primary_key=True)
    location = relationship("DarwinLocation", uselist=False, lazy="select")

    # main

    main_rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, index=True, primary_key=True)
    main_rid_constraint = ForeignKeyConstraint(("main_rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    main_original_wt = Column(VARCHAR(18), nullable=False, index=True, primary_key=True)


    main_schedule = relationship("DarwinSchedule", foreign_keys=(main_rid,), uselist=False)

    # TODO: foreign key tomfuckery
    fkey_main_schedule_loc = ForeignKeyConstraint(
        (main_rid, main_original_wt, tiploc),
        ("darwin_schedule_locations.rid", "darwin_schedule_locations.original_wt", "darwin_schedule_locations.tiploc"), ondelete="CASCADE"
    )
    main_schedule_loc = relationship("DarwinScheduleLocation", foreign_keys=(main_rid, main_original_wt, tiploc), viewonly=True)


    # associated

    assoc_rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, index=True, primary_key=True)
    assoc_rid_constraint = ForeignKeyConstraint(("assoc_rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    assoc_original_wt = Column(VARCHAR(18), nullable=False, index=True, primary_key=True)

    assoc_schedule = relationship("DarwinSchedule", foreign_keys=(assoc_rid,), uselist=False)

    # TODO: more foreign key tomfuckery
    fkey_assoc_schedule_loc = ForeignKeyConstraint(
        (assoc_rid, assoc_original_wt, tiploc),
        ("darwin_schedule_locations.rid", "darwin_schedule_locations.original_wt", "darwin_schedule_locations.tiploc"), ondelete="CASCADE"
    )
    assoc_schedule_loc = relationship("DarwinScheduleLocation", foreign_keys=(assoc_rid, assoc_original_wt, tiploc), viewonly=True)


class DarwinScheduleStatus(Base):
    __tablename__ = "darwin_schedule_status"
    __table_args__ = (
        #ForeignKeyConstraint(("rid", "original_wt", "tiploc"), ("darwin_schedule_locations.rid", "darwin_schedule_locations.original_wt", "darwin_schedule_locations.tiploc"), ondelete="CASCADE"),
        UniqueConstraint("rid", "tiploc", "original_wt"),
    )

    rid = Column(CHAR(15), nullable=False, primary_key=True) # Technically a schedule ref but putting it as a foreign key screws everything up sorry
    tiploc = Column(VARCHAR(7), ForeignKey("darwin_locations.tiploc"), index=True, primary_key=True)
    location = relationship("DarwinLocation", uselist=False)
    original_wt = Column(VARCHAR(18), index=True, primary_key=True)

    ta = Column(TIME, default=None, index=True)
    tp = Column(TIME, default=None, index=True)
    td = Column(TIME, default=None, index=True)

    ta_source = Column(VARCHAR, default=None)
    tp_source = Column(VARCHAR, default=None)
    td_source = Column(VARCHAR, default=None)

    ta_type = Column(VARCHAR(1), default=None)
    tp_type = Column(VARCHAR(1), default=None)
    td_type = Column(VARCHAR(1), default=None)

    ta_delayed = Column(BOOLEAN, nullable=False)
    tp_delayed = Column(BOOLEAN, nullable=False)
    td_delayed = Column(BOOLEAN, nullable=False)

    plat = Column(VARCHAR, default=None)
    plat_suppressed = Column(BOOLEAN)
    plat_cis_suppressed = Column(BOOLEAN)
    plat_confirmed = Column(BOOLEAN)
    plat_source = Column(VARCHAR)

    length = Column(SMALLINT)

    #schedule_location = relationship("DarwinScheduleLocation", back_populates="status")

    def format_platform(self):
        return ("*"*self.plat_suppressed) + (self.plat or ' ') + ("." * self.plat_confirmed)

    def __repr__(self):
        return "<DarwinScheduleStatus {}>".format(self.format_platform())


class DarwinMessage(Base):
    __tablename__ = "darwin_messages"
    message_id = Column(INTEGER, nullable=False, primary_key=True, unique=True, index=True)
    category = Column(VARCHAR, nullable=False)
    severity = Column(SMALLINT, nullable=False)
    suppress = Column(BOOLEAN, nullable=False)
    stations = Column(ARRAY(VARCHAR(3)), nullable=False, index=True)
    message = Column(VARCHAR, nullable=False)

    def serialise(self, recurse=False):
        return OrderedDict([
            ("id", self.message_id),
            ("category", self.category),
            ("severity", self.severity),
            ("suppress", self.suppress),
            ("stations", self.stations),
            ("message", self.message)
        ])


class LastReceivedSequence(Base):
    __tablename__ = "last_received_sequence"
    id = Column(SMALLINT, nullable=False, unique=True, primary_key=True)
    sequence = Column(INTEGER, nullable=False)
    time_acquired = Column(TIMESTAMP, nullable=False)

