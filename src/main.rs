use log::info;
#[allow(unused_imports)]
use mysql::prelude::Queryable;
use mysql::{params, Conn, Opts};
use serde::{Deserialize, Serialize};

use std::thread::sleep;
use std::time;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vkclient::{Version, VkApi, VkApiBuilder, VkApiError};

#[derive(Clone, Debug)]
struct AdvertInfo {
    id: i32,
    token: String,
    message: String,
    attachments: String,
    group_ids: String,
    timer: i32,
    last_posted: i32,
}

impl AdvertInfo {
    pub fn get_owner_ids_vec(&self) -> Vec<i32> {
        self.group_ids
            .clone()
            .trim()
            .split(", ")
            .map(my_parse)
            .collect()
    }

    pub fn load(conn: &mut Conn) -> Vec<Self> {
        let ads: Vec<AdvertInfo> = conn.query_map("SELECT id, token, message, attachments, group_ids, timer, last_posted FROM rpads WHERE untill_date > UNIX_TIMESTAMP()",
                                                  |(id, token, message, attachments, group_ids, timer, last_posted)|
                                                      AdvertInfo {
                                                          id,
                                                          token,
                                                          message,
                                                          attachments,
                                                          group_ids,
                                                          timer,
                                                          last_posted
                                                      }
        ).unwrap();
        ads
    }
}

fn my_parse(x: &str) -> i32 {
    let out = x.replace(',', "");
    out.parse().unwrap_or(0)
}

type WallPostResult = Result<WallPostResponse, VkApiError>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init_timed();
    macro_rules! aw {
        ($e: expr) => {
            tokio_test::block_on($e)
        };
    }

    let url = "mysql://vk_rp_ad:1243@localhost:3306/master";
    let opts = Opts::from_url(url)?;
    let mut conn = Conn::new(opts)?;
    let mut ads = AdvertInfo::load(&mut conn);

    loop {
        for ad in &ads {
            if time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("TIME WENT BACKWARDS WHAT THE FUCK")
                .as_secs()
                >= (ad.last_posted + ad.timer) as u64
            {
                send_ads(ad.clone(), &mut conn).await;
                info!("sleeping for 10s before next advertisement");
                sleep(Duration::new(10, 0));
            } else {
                info!("Nothing to do on ID: {}", ad.id);
            }
        }
        ads = AdvertInfo::load(&mut conn);
        info!("sleeping for 120s!");
        sleep(Duration::new(120, 0))
    }

    Ok(())
}

async fn send_ads(ad_info: AdvertInfo, db_conn: &mut Conn) {
    let owner_ids = ad_info.get_owner_ids_vec();
    let client: VkApi = VkApiBuilder::new(ad_info.token.clone()).into();
    for id in owner_ids {
        if id != 0 {
            info!("ID: {} posting in ID: {}", ad_info.id, id);
            let res = client
                .send_request_with_version::<WallPostResponse, WallPostRequest, &str>(
                    "wall.post",
                    WallPostRequest {
                        owner_id: -id,
                        message: &ad_info.message.clone(),
                        attachments: &ad_info.attachments.clone(),
                    },
                    Version(5, 131),
                )
                .await;
            info!(
                "ID: {} post response: {}",
                ad_info.id,
                match res {
                    Ok(v) => v.post_id.to_string(),
                    Err(e) => e.to_string()
                }
            );
            sleep(Duration::new(3, 0));
        }
    }
    db_conn.exec_drop("UPDATE rpads SET last_posted = :last_posted WHERE id = :id", params! {
            "last_posted" => SystemTime::now().duration_since(UNIX_EPOCH).expect("TIME WENT BACKWARDS WTF").as_secs(),
            "id" => ad_info.id
        }).unwrap();
}

#[derive(Serialize)]
struct WallPostRequest<'a> {
    owner_id: i32,
    message: &'a str,
    attachments: &'a str,
}

#[derive(Deserialize)]
struct WallPostResponse {
    post_id: i32,
}
