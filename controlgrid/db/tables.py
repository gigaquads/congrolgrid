import sqlalchemy as sa

from appyratus.utils.time_utils import TimeUtils


def jobs(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "jobs",
        metadata,
        *[
            sa.Column("job_id", sa.String(), primary_key=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, index=True),
            sa.Column("command", sa.String(length=100), nullable=False),
            sa.Column("args", sa.JSON(), nullable=False, default=[]),
            sa.Column(
                "status", sa.String(length=20), nullable=False, index=True
            ),
            sa.Column("tag", sa.String(length=100), index=True),
            sa.Column("stream", sa.String(length=100), index=True),
            sa.Column("timeout", sa.Float()),
            sa.Column("pid", sa.Integer()),
            sa.Column("exit_code", sa.Integer()),
            sa.Column("error", sa.String()),
        ]
    )


# def get_job_by_id(
#     cls, job_id: str, db_session: Session = Depends(get_db_session)
# ) -> Optional[Job]:
#     dao = db_session.query(cls).get(job_id)
#     if dao:
#         return Job(
#             dao.job_id,
#             dao.command,
#             dao.args,
#             dao.tag,
#             dao.timeout,
#             dao.exit_code,
#             dao.pid,
#             dao.error,
#         )
#     return None
