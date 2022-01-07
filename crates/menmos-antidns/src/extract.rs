use std::{convert::TryFrom, net::Ipv4Addr};

use nom::{
    character::complete::{char, digit1},
    combinator::map_res,
    sequence::tuple,
    IResult,
};
use snafu::{ensure, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    NameWithoutIp,
    FailedToParse { message: String },
    IncompleteParse,
}

fn digit(i: &str) -> IResult<&str, u8> {
    map_res(digit1, |int_val: &str| int_val.parse::<u8>())(i)
}

fn ip_address(i: &str) -> IResult<&str, Ipv4Addr> {
    map_res(
        tuple((digit, char('-'), digit, char('-'), digit, char('-'), digit)),
        |(a, _, b, _, c, _, d)| Ipv4Addr::try_from([a, b, c, d]),
    )(i)
}

pub fn ip_address_from_url<S: AsRef<str>>(url: S) -> Result<Ipv4Addr, Error> {
    let splitted: Vec<_> = url.as_ref().split('.').collect();
    ensure!(splitted.len() >= 2, NameWithoutIpSnafu);

    let ip_segment = *splitted.first().unwrap();
    let (rest, ip) = ip_address(ip_segment).map_err(|e| Error::FailedToParse {
        message: e.to_string(),
    })?;
    ensure!(rest.is_empty(), IncompleteParseSnafu);

    Ok(ip)
}
