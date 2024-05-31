//! Users in Hopsworks Feature Store.
//!
//! No user methods are implemented in the rust SDK as of now,
//! but it can be used to get information about the user that is logged in to the SDK.
use serde::{Deserialize, Serialize};

use crate::hopsworks_internal::platform::users::UserDTO;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub status: i32,
    pub tos: bool,
    pub two_factor: bool,
    pub tours_state: i32,
    pub max_num_projects: i32,
    pub num_created_projects: Option<i32>,
    pub test_user: bool,
    pub num_active_projects: i32,
    pub num_remaining_projects: i32,
}

impl User {
    pub fn new_from_dto(user_dto: UserDTO) -> Self {
        Self {
            email: user_dto.email,
            first_name: user_dto.first_name,
            last_name: user_dto.last_name,
            status: user_dto.status,
            tos: user_dto.tos,
            two_factor: user_dto.two_factor,
            tours_state: user_dto.tours_state,
            max_num_projects: user_dto.max_num_projects,
            num_created_projects: user_dto.num_created_projects,
            test_user: user_dto.test_user,
            num_active_projects: user_dto.num_active_projects,
            num_remaining_projects: user_dto.num_remaining_projects,
        }
    }
}

impl From<UserDTO> for User {
    fn from(user_dto: UserDTO) -> Self {
        User::new_from_dto(user_dto)
    }
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