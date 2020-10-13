import sqlalchemy
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint, CHAR, VARCHAR, JSON, SMALLINT, INTEGER, DATE, BOOLEAN, TIMESTAMP, TIME, ARRAY
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DarwinOperator(Base):
    __tablename__ = "darwin_operators"

    operator = Column(CHAR(2), primary_key=True, unique=True, index=True)
    operator_name = Column(VARCHAR)
    url = Column(VARCHAR)

    def __repr__(self):
        return "<DarwinOperator {} - {}>".format(self.operator, self.operator_name)

class DarwinReason(Base):
    __tablename__ = "darwin_reasons"
    id = Column(SMALLINT, unique=True, primary_key=True, index=True)
    type = Column(CHAR(1), unique=True, primary_key=True)
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

    def __repr__(self):
        return "<DarwinLocation {} - {}>".format(self.tiploc, self.name_short)


class DarwinSchedule(Base):
    __tablename__ = "darwin_schedules"
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

    origins = Column(JSON, nullable=False)
    destinations = Column(JSON, nullable=False)

    delay_reason = Column(JSON, default=None)
    cancel_reason = Column(JSON, default=None)

    uid_ssd_unique_constraint = UniqueConstraint("uid", "ssd")

    locations = relationship("DarwinScheduleLocation", lazy="select", primaryjoin="foreign(DarwinSchedule.rid)==DarwinScheduleLocation.rid")

    origins_rel = relationship("DarwinScheduleLocation", lazy="joined", primaryjoin="and_(foreign(DarwinSchedule.rid)==DarwinScheduleLocation.rid, DarwinScheduleLocation.loc_type.like('%OR'))")
    destinations_rel = relationship("DarwinScheduleLocation", lazy="joined", primaryjoin="and_(foreign(DarwinSchedule.rid)==DarwinScheduleLocation.rid, DarwinScheduleLocation.loc_type.like('%DT'))")


    def __repr__(self):
        #return "<DarwinSchedule {}/{} ({}) {} {}\n{}\n>".format(self.ssd, self.uid, self.rid, self.signalling_id, self.operator, self.locations)
        return "{}/{} (r. {} rs. {}) {}".format(self.ssd, self.uid, self.rid, self.rid, self.rsid, self.operator_id)


class DarwinAssociation(Base):
    __tablename__ = "darwin_associations"

    category = Column(CHAR(2), nullable=False)
    tiploc = Column(VARCHAR(3), ForeignKey("darwin_locations.tiploc"), nullable=False, index=True, primary_key=True)
    location = relationship("DarwinLocation", uselist=False, lazy="select")

    # main

    main_rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, index=True, primary_key=True)
    main_rid_constraint = ForeignKeyConstraint(("main_rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    main_original_wt = Column(VARCHAR(18), nullable=False, index=True, primary_key=True)
    unique_main_rid_owt = UniqueConstraint("main_rid", "main_original_wt")

    main_schedule = relationship("DarwinSchedule", foreign_keys=(main_rid,), uselist=False)

    # TODO: foreign key tomfuckery
    fkey_main_schedule_loc = ForeignKeyConstraint(
        (main_rid, main_original_wt),
        ("darwin_schedule_locations.rid", "darwin_schedule_locations.original_wt")
    )
    main_schedule_loc = relationship("DarwinScheduleLocation", foreign_keys=(main_rid,main_original_wt), viewonly=True)

    # associated

    assoc_rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, index=True, primary_key=True)
    assoc_rid_constraint = ForeignKeyConstraint(("rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    assoc_original_wt = Column(VARCHAR(13), nullable=False, index=True, primary_key=True)
    unique_assoc_rid_owt = UniqueConstraint("assoc_rid", "assoc_original_wt")

    associated_schedule = relationship("DarwinSchedule", foreign_keys=(assoc_rid,), uselist=False)

    # TODO: more foreign key tomfuckery
    fkey_assoc_schedule_loc = ForeignKeyConstraint(
        (assoc_rid, assoc_original_wt),
        ("darwin_schedule_locations.rid", "darwin_schedule_locations.original_wt")
    )
    assoc_schedule_loc = relationship("DarwinScheduleLocation", foreign_keys=(main_rid, main_original_wt), viewonly=True)


class DarwinScheduleLocation(Base):
    __tablename__ = "darwin_schedule_locations"

    rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, primary_key=True)
    rid_constraint = ForeignKeyConstraint(("rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    schedule = relationship("DarwinSchedule", uselist=False, lazy="joined", innerjoin=True)

    index = Column(SMALLINT, primary_key=True)
    loc_type = Column(VARCHAR(4), nullable=False, name="type")

    tiploc = Column(VARCHAR(7), ForeignKey("darwin_locations.tiploc"), nullable=False, index=True)
    location = relationship("DarwinLocation", uselist=False)

    activity = Column(VARCHAR(12), nullable=False)
    original_wt = Column(VARCHAR(18))

    pta = Column(TIMESTAMP, default=None)
    wta = Column(TIMESTAMP, default=None, index=True)
    wtp = Column(TIMESTAMP, default=None, index=True)
    ptd = Column(TIMESTAMP, default=None)
    wtd = Column(TIMESTAMP, default=None, index=True)

    cancelled = Column(BOOLEAN, nullable=False, default=False)
    rdelay = Column(SMALLINT, nullable=False, default=0)

    status = relationship("DarwinScheduleStatus", uselist=False, lazy="joined", innerjoin=True)

    associated_to = relationship("DarwinAssociation", lazy="joined", primaryjoin="and_(foreign(DarwinScheduleLocation.rid)==DarwinAssociation.main_rid, foreign(DarwinScheduleLocation.original_wt)==DarwinAssociation.main_original_wt)")
    associated_from = relationship("DarwinAssociation", lazy="joined", primaryjoin="and_(foreign(DarwinScheduleLocation.rid)==DarwinAssociation.assoc_rid, foreign(DarwinScheduleLocation.original_wt)==DarwinAssociation.assoc_original_wt)")

    def __repr__(self):
        return "<DarwinScheduleLocation {}/{}/{} wta {} wtd {} s {} f - t ->".format(self.rid, self.tiploc, self.index, self.wta, self.wtd, self.status)


class DarwinScheduleStatus(Base):
    __tablename__ = "darwin_schedule_status"
    __table_args__ = (
        ForeignKeyConstraint(("rid", "original_wt"), ("darwin_schedule_locations.rid", "darwin_schedule_locations.original_wt"), ondelete="CASCADE"),
    )

    rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, primary_key=True)
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

    schedule_location = relationship("DarwinScheduleLocation", back_populates="status")

    UniqueConstraint("rid", "tiploc", "original_wt")


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


class LastReceivedSequence(Base):
    __tablename__ = "last_received_sequence"
    id = Column(SMALLINT, nullable=False, unique=True, primary_key=True)
    sequence = Column(INTEGER, nullable=False)
    time_acquired = Column(TIMESTAMP, nullable=False)

