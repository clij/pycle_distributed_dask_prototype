from dask_jobqueue import SLURMCluster
from distributed import Client


def build_slurm_cluster(config: dict):
    jobs = config.get("jobs", 1)
    queue = config.get("queue", "cpu")
    processes = config.get("processes", 1)
    cores = config.get("cores", 1)
    memory = config.get("memory", "4GB")
    interface = config.get("interface", None)
    walltime = config.get('walltime', "01:00:00")
    job_extras = config.get("job_extra", [])
    job_script_prologue = config.get("env_setup", [])

    cluster = SLURMCluster(
        queue=queue,
        processes=processes,
        cores=cores,
        memory=memory,
        interface=interface,
        walltime=walltime,
        job_extra_directives=job_extras,
        job_script_prologue=job_script_prologue
    )

    cluster.adapt(minimum=0, maximum=jobs)
    return Client(cluster)
