use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserDTO {
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub status: i32,
    pub tos: bool,
    pub two_factor: bool,
    pub tours_state: i32,
    pub max_num_projects: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_created_projects: Option<i32>,
    pub test_user: bool,
    pub num_active_projects: i32,
    pub num_remaining_projects: i32,
}
