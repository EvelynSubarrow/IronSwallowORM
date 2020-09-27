import sqlalchemy
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint, CHAR, VARCHAR, JSON, SMALLINT, DATE, BOOLEAN, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

engine = sqlalchemy.create_engine("postgresql+psycopg2://user:@/ironswallow_evelyn")

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

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

    def __repr__(self):
        return "<DarwinSchedule {}/{} ({}) {} {}>".format(self.ssd, self.uid, self.rid, self.signalling_id, self.operator)


class DarwinScheduleLocation(Base):
    __tablename__ = "darwin_schedule_locations"
    rid = Column(CHAR(15), ForeignKey("darwin_schedules.rid"), nullable=False, primary_key=True)
    rid_constraint = ForeignKeyConstraint(("rid",), ("darwin_schedules.rid",), ondelete="CASCADE")
    index = Column(SMALLINT, primary_key=True)
    loc_type = Column(VARCHAR(4), nullable=False, name="type")
    tiploc = Column(VARCHAR(7), ForeignKey("darwin_locations.tiploc"), nullable=False, index=True)
    activity = Column(VARCHAR(12), nullable=False)
    original_wt = Column(VARCHAR(18))

    pta = Column(TIMESTAMP, default=None)
    wta = Column(TIMESTAMP, default=None, index=True)
    wtp = Column(TIMESTAMP, default=None, index=True)
    ptd = Column(TIMESTAMP, default=None)
    wtd = Column(TIMESTAMP, default=None, index=True)

    cancelled = Column(BOOLEAN, nullable=False, default=False)
    rdelay = Column(SMALLINT, nullable=False, default=0)

    schedule = relationship("DarwinSchedule")

    def __repr__(self):
        return "<DarwinScheduleLocation {}/{}/{} wta {} wtd {}>".format(self.rid, self.tiploc, self.index, self.wta, self.wtd)

