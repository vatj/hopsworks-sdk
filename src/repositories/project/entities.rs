use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProjectAndUserDTO {
    user: ProjectUserDTO,
    pub project: ProjectDTO,
    #[serde(rename = "projectTeamPK")]
    project_team_pk: ProjectTeamPkDTO,
    team_role: String,
    timestamp: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDTO {
    archived: bool,
    logs: bool,
    pub id: i32,
    pub name: String,
    owner: ProjectUserDTO,
    python_environment: PythonEnvironmentDTO,
    created: String,
    retention_period: String,
    payment_type: String,
    description: Option<String>,
    kafka_max_num_topics: i32,
    last_quota_update: String,
    docker_image: String,
    creation_status: String,
    inode: InodeDTO,
    payment_type_string: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QuotaDTO {
    hdfs_usage_in_bytes: i32,
    hdfs_quota_in_bytes: i32,
    hdfs_ns_count: i32,
    hdfs_ns_quota: i32,
    hive_hdfs_usage_in_bytes: i32,
    hive_hdfs_quota_in_bytes: i32,
    hive_hdfs_ns_count: i32,
    hive_hdfs_ns_quota: i32,
    featurestore_hdfs_usage_in_bytes: i32,
    featurestore_hdfs_quota_in_bytes: i32,
    featurestore_hdfs_ns_count: i32,
    featurestore_hdfs_ns_quota: i32,
    yarn_quota_in_secs: f64,
    yarn_used_quota_in_secs: f64,
    kafka_max_num_topics: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProjectTeamPkDTO {
    project_id: i32,
    team_member: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUserDTO {
    uid: i32,
    username: String,
    email: String,
    fname: String,
    lname: String,
    title: String,
    status: String,
    isonline: i8,
    mode: String,
    password_changed: String,
    max_num_projects: i32,
    num_created_projects: i32,
    num_active_projects: i32,
    tours_state: i32,
    bbc_group_collection: Vec<BbcGroupCollectionDTO>,
    group_names: Vec<GroupNameDTO>,
    status_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GroupNameDTO {
    group_name: String,
    group_desc: String,
    gid: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BbcGroupCollectionDTO {
    group_name: String,
    group_desc: String,
    gid: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PythonEnvironmentDTO {
    id: i32,
    project_id: i32,
    python_version: String,
    jupyter_conflicts: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InodeDTO {
    #[serde(rename = "inodePK")]
    inode_p_k: InodePkDTO,
    id: i32,
    modification_time: i64,
    access_time: i64,
    hdfs_user: HdfsUserDTO,
    hdfs_group: HdfsGroupDTO,
    permission: i32,
    quota_enabled: bool,
    under_construction: bool,
    meta_status: String,
    dir: bool,
    children_num: i32,
    size: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InodePkDTO {
    parent_id: i32,
    name: String,
    partition_id: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HdfsUserDTO {
    id: i32,
    name: String,
    project: String,
    username: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HdfsGroupDTO {
    id: i32,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SingleProjectDTO {
    project_id: i32,
    pub project_name: String,
    owner: String,
    description: Option<String>,
    docker_image: String,
    retention_period: String,
    created: String,
    archived: bool,
    services: Vec<String>,
    project_team: Vec<ProjectAndUserDTO>,
    inodeid: i32,
    quotas: QuotaDTO,
    hops_examples: String,
    is_preinstalled_docker_image: bool,
    is_old_docker_image: bool,
    creation_status: String,
}
