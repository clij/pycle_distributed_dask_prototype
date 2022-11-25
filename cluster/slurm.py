import tempfile

import dask
from dask_jobqueue import SLURMCluster
from distributed import Client


def build_slurm_cluster(config: dict):
    jobs = config.get("jobs", 1)
    queue = config.get("queue", "cpu")
    processes = config.get("processes", 1)
    cores = config.get("cores", 1)
    memory = config.get("memory", "4GB")
    walltime = config.get('walltime', "01:00:00")
    local_directory = config.get('local_directory', '$TMPDIR')
    temp_directory = config.get('temp_directory', '$TMPDIR')
    job_extras = config.get("job_extra", [])
    job_script_prologue = config.get("env_setup", [])

    # Some /tmp doesnt map across
    tempfile.tempdir = temp_directory
    dask.config.set({'temporary_directory': temp_directory})

    cluster = SLURMCluster(
        queue=queue,
        processes=processes,
        cores=cores,
        memory=memory,
        walltime=walltime,
        local_directory=local_directory,
        job_extra_directives=job_extras,
        job_script_prologue=job_script_prologue
    )

    #cluster.adapt(minimum=0, maximum=jobs)
    cluster.scale(jobs)
    return Client(cluster)
