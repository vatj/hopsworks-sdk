use serde::{Deserialize, Serialize};

use crate::platform::user::User;

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

impl From<User> for UserDTO {
    fn from(user: User) -> Self {
        UserDTO::new_from_user(user)
    }
}

impl UserDTO {
    pub fn new_from_user(user: User) -> Self {
        Self {
            email: user.email,
            first_name: user.first_name,
            last_name: user.last_name,
            status: user.status,
            tos: user.tos,
            two_factor: user.two_factor,
            tours_state: user.tours_state,
            max_num_projects: user.max_num_projects,
            num_created_projects: None,
            test_user: user.test_user,
            num_active_projects: user.num_active_projects,
            num_remaining_projects: user.num_remaining_projects,
        }
    }
}
