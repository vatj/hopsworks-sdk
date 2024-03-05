use clap::Subcommand;

#[derive(Debug, Subcommand)]
#[command(flatten_help = true)]
pub enum JobSubCommand {
    /// Get metadata information about a job
    #[command(arg_required_else_help = true)]
    Info {
        /// Job name in the current project
        #[arg(short, long, required = true)]
        name: String,
    },
    /// List all jobs in the current project
    List {},
    #[command(arg_required_else_help = true)]
    ListExecutions {
        /// Job name in the current project or the project must
        /// be specified with the `--project` flag
        #[arg(short, long, required = true)]
        name: String,
        // Fetch only active executions of the job meaning
        // executions that are not yet finished or failed
        // Possible Execution Status are:
        // `RUNNING`, `INITIALIZING`, `SUBMITTED`, `ACCEPTED`
        #[arg(long)]
        active: bool,
    },
    /// Trigger the execution of a job with a given name and optional args
    #[command(arg_required_else_help = true)]
    Run {
        /// Job name in the current project
        #[arg(short, long, required = true)]
        name: String,
        /// Args string for the job, e.g. `"--arg1 value1 --arg2 value2"`
        /// with quotes
        #[arg(long)]
        args: Option<String>,
        /// Await termination of the job execution
        /// If not specified, defaults to `false`
        #[arg(long, default_value = "false")]
        await_termination: bool,
        /// Download logs of the job execution after it has finished.
        /// Requires `await_termination` to be `true`.
        /// If not specified, defaults to `false`
        #[arg(long, default_value = "false")]
        download_logs: bool,
    },
}

pub async fn show_job_info(job_name: &str, project: hopsworks::platform::project::Project) {
    println!(
        "Job Info for job {:?}:\n{:#?}",
        job_name,
        project.get_job(job_name).await
    );
}

pub async fn show_list_jobs(project: hopsworks::platform::project::Project) {
    println!("Fetching all jobs within project {}:", project.name(),);
    let jobs: Vec<hopsworks::platform::job::Job> = project
        .get_jobs()
        .await
        .unwrap_or_else(|_| panic!("Failed to fetch jobs for project {}.\n", project.name()));
    jobs.iter().for_each(|job| {
        println!(
            "id: {}, name: {}, type: {}, created at: {}",
            job.id(),
            job.name(),
            job.job_type(),
            job.creation_time()
        );
    });
}

pub async fn show_list_executions(
    project: hopsworks::platform::project::Project,
    name: &str,
    _active: bool,
) {
    let job = project.get_job(name).await.unwrap_or_else(|_| {
        panic!(
            "Job {} not found in project {}, check fo typo or that you are in the right project.",
            name,
            project.name()
        )
    });
    let executions = job
        .get_executions()
        .await
        .unwrap_or_else(|_| panic!("Failed to fetch job executions for {}.\n", name));
    println!("Listing all executions for job {}: {:#?}", name, executions);
}

pub async fn show_run_job(
    project: hopsworks::platform::project::Project,
    name: &str,
    args: &Option<String>,
    await_termination: bool,
    download_logs: bool,
) {
    let job = project.get_job(name).await.unwrap_or_else(|_| {
        panic!(
            "Job {} not found in project {}, check fo typo or that you are in the right project.",
            name,
            project.name()
        )
    });
    println!("Executing job {:#?}...", name);
    let job_exec = job
        .run(args.as_deref(), false)
        .await
        .unwrap_or_else(|_| panic!("Failed to run job {}.", name));
    if await_termination {
        job_exec
            .await_termination()
            .await
            .unwrap_or_else(|_| panic!("Failed to await termination of job {}.", name));
    }
    if download_logs {
        job_exec
            .download_logs(None)
            .await
            .unwrap_or_else(|_| panic!("Failed to download logs for job {}.", name));
    }
}
