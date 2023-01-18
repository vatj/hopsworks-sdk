#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureView {
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub query: Query,
    pub transformation_functions: HashMap<&str, TransformationFunction>,
}
