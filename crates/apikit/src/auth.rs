use warp::Filter;

use crate::reject;

pub async fn validate_password<E: AsRef<str>, A: AsRef<str>>(
    actual_password: Option<A>,
    expected_password: E,
) -> Result<(), warp::Rejection> {
    if actual_password.is_none() || expected_password.as_ref() != actual_password.unwrap().as_ref()
    {
        Err(warp::reject::custom(reject::Forbidden))
    } else {
        Ok(())
    }
}

pub fn authenticated(
    expected_password: String,
) -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::header::optional::<String>("authorization")
        .and(warp::any().map(move || expected_password.clone()))
        .and_then(validate_password)
        .and(warp::any())
        .untuple_one()
}
