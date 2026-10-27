use json_rpc_2_server::{RegistrationError, RpcServer};
#[cfg(any(feature = "tms", feature = "tracking"))]
use serde_json::Value;
use serde_json::json;

#[cfg(any(feature = "tms", feature = "tracking"))]
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required})
}

pub fn install(rpc: &RpcServer) -> Result<(), RegistrationError> {
    rpc.document_method(
        "echo",
        json!({"type":["object","array"],"description":"Returns supplied params unchanged."}),
        json!({"message":"example"}),
    )?;
    rpc.document_method(
        "identify",
        json!({"type":["object","array"],"description":"No required params; supplied params are ignored."}),
        json!({}),
    )?;
    #[cfg(feature = "tms")]
    {
        let text = json!({"type":"string","minLength":1,"pattern":"\\S"});
        rpc.document_method("login", object(json!({
            "phone":text, "password":text,
            "device":object(json!({"deviceId":text}), &["deviceId"])
        }), &["phone","password","device"]), json!({
            "phone":"+380000000000","password":"mock-password","device":{"deviceId":"mock-device"}
        }))?;
        for method in ["resume", "session.info"] {
            rpc.document_method(
                method,
                object(json!({"sessionId":text}), &["sessionId"]),
                json!({"sessionId":"mock-session-id"}),
            )?;
        }
        rpc.document_method(
            "getShift",
            object(json!({"accessToken":text}), &["accessToken"]),
            json!({"accessToken":"mock-access-token"}),
        )?;
        let time = json!({"not":{"type":"null"},"description":"Non-null value forwarded to upstream; example uses an ISO 8601 timestamp."});
        rpc.document_method("startShift", object(json!({
            "accessToken":text,"deviceTime":time,"sendTime":time,"vehicleId":text,
            "lat":{"description":"Optional value forwarded unchanged to upstream."},
            "lng":{"description":"Optional value forwarded unchanged to upstream."}
        }), &["accessToken","deviceTime","sendTime","vehicleId"]), json!({
            "accessToken":"mock-access-token","deviceTime":"2026-01-01T12:00:00Z",
            "sendTime":"2026-01-01T12:00:01Z","vehicleId":"mock-vehicle-id","lat":50.45,"lng":30.52
        }))?;
        rpc.document_method(
            "finishShift",
            object(
                json!({
                    "accessToken":text,"deviceTime":time
                }),
                &["accessToken", "deviceTime"],
            ),
            json!({
                "accessToken":"mock-access-token","deviceTime":"2026-01-01T13:00:00Z"
            }),
        )?;
    }
    Ok(())
}

#[cfg(feature = "tracking")]
pub fn install_tracking(rpc: &RpcServer, max_batch: usize) -> Result<(), RegistrationError> {
    let mut data_properties = serde_json::Map::new();
    let mut data = serde_json::Map::new();
    let mut required = Vec::new();
    for (kind, names, example) in [
        (
            "number",
            "latitude longitude accuracy rawLatitude rawLongitude rawAccuracy altitude speed speedAccuracy heading headingAccuracy motionDisplacement10s motionSignalScore sourceTrustScore stationaryAnchorDistanceMeters stepDistanceMeters totalDistanceMeters driftMeters totalDriftMeters rawSmoothedDeltaMeters",
            json!(0.0),
        ),
        (
            "boolean",
            "motionSpeedSaysMoving motionHeadingStable motionTransitionBlocked motionSourceUsed motionUpdatedBySignalOnly adaptivePollingEnabled sourceTrustSuspicious positionHeldByNoGnss positionHeldByTrust stationaryLockApplied deadReckoningAvailable deadReckoningMode isMocked",
            json!(false),
        ),
        (
            "string",
            "source motionState motionTuningProfile motionTuningProfileDefine motionSignalState pollingProfile positionQualityMode timestamp",
            json!("mock"),
        ),
        ("integer", "watchdogRestartCount", json!(0)),
    ] {
        for name in names.split_whitespace() {
            data_properties.insert(name.into(), json!({"type":kind}));
            data.insert(name.into(), example.clone());
            required.push(name);
        }
    }
    let uuid = "00000000-0000-4000-8000-000000000001";
    for name in ["brandId", "driverId"] {
        data_properties.insert(name.into(), json!({"type":"string","format":"uuid"}));
        data.insert(name.into(), json!(uuid));
        required.push(name);
    }
    for name in ["shiftId", "vehicleId"] {
        data_properties.insert(
            name.into(),
            json!({"type":["string","null"],"format":"uuid"}),
        );
    }
    for name in [
        "accuracyConfidence",
        "accuracyReductionFactor",
        "accuracyCapMeters",
    ] {
        data_properties.insert(name.into(), json!({"type":["number","null"]}));
    }
    for name in ["motionTransitionBlockReason", "sourceTrustReason"] {
        data_properties.insert(name.into(), json!({"type":["string","null"]}));
    }
    for name in ["activityMotion", "sensorMotion", "gnss"] {
        data_properties.insert(name.into(), json!({}));
    }
    let kinematics = object(
        json!({
            "speedMps":{"type":"number"}, "speedAccuracyMps":{"type":"number"},
            "speedReliable":{"type":"boolean"}, "speedIndicatesStationary":{"type":"boolean"},
            "speedIndicatesMoving":{"type":"boolean"}, "headingReliable":{"type":"boolean"},
            "headingDeg":{"type":["number","null"]}, "headingAccuracyDeg":{"type":["number","null"]}
        }),
        &[
            "speedMps",
            "speedAccuracyMps",
            "speedReliable",
            "speedIndicatesStationary",
            "speedIndicatesMoving",
            "headingReliable",
        ],
    );
    data_properties.insert("kinematics".into(), kinematics);
    required.push("kinematics");
    data.insert(
        "kinematics".into(),
        json!({
            "speedMps":0.0,"speedAccuracyMps":0.0,"speedReliable":true,
            "speedIndicatesStationary":true,"speedIndicatesMoving":false,"headingReliable":false
        }),
    );
    data.insert("timestamp".into(), json!("2026-01-01T12:00:00Z"));
    data.insert("source".into(), json!("gps"));
    data.insert("isMocked".into(), json!(true));
    let mut record = object(
        json!({
            "id":{"type":"string","format":"uuid"},
            "at":{"type":"string","format":"date-time"},
            "sent":{"type":"string","format":"date-time"},
            "data":object(Value::Object(data_properties), &required)
        }),
        &["id", "at", "sent", "data"],
    );
    record["additionalProperties"] = json!(false);
    let example =
        json!({"id":uuid,"at":"2026-01-01T12:00:00Z","sent":"2026-01-01T12:00:01Z","data":data});
    let schema = json!({
        "oneOf":[record,{"type":"array","items":record,"minItems":1,"maxItems":max_batch}],
        "description":"One tracking record or a nonempty batch. Example values are synthetic; do not submit them to production storage."
    });
    for name in ["sendLocation", "tracking.sendLocation"] {
        rpc.document_method(name, schema.clone(), example.clone())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> RpcServer {
        let rpc = RpcServer::new();
        for name in ["echo", "identify"] {
            rpc.register(name, |params| async { Ok(params) }).unwrap();
        }
        #[cfg(feature = "tms")]
        for name in [
            "login",
            "resume",
            "session.info",
            "getShift",
            "startShift",
            "finishShift",
        ] {
            rpc.register(name, |params| async { Ok(params) }).unwrap();
        }
        install(&rpc).unwrap();
        #[cfg(feature = "tms")]
        super::super::register_tms_aliases(&rpc).unwrap();
        #[cfg(feature = "tracking")]
        {
            for name in ["sendLocation", "tracking.sendLocation"] {
                rpc.register(name, |params| async { Ok(params) }).unwrap();
            }
            install_tracking(&rpc, 7).unwrap();
        }
        rpc
    }

    #[test]
    fn every_enabled_method_has_a_schema_and_synthetic_request() {
        for description in registry().method_descriptions() {
            assert!(description.params_schema.is_some(), "{}", description.name);
            let request = description.example_request.unwrap();
            assert_eq!(request["method"], description.name);
            assert_eq!(request["jsonrpc"], "2.0");
            assert!(request["params"].is_object());
        }
    }

    #[cfg(feature = "tracking")]
    #[test]
    fn tracking_example_matches_plugin_types_and_required_fields() {
        use tms_server_tracking::tracking::TrackingRecord;
        let description = registry()
            .method_descriptions()
            .into_iter()
            .find(|description| description.name == "sendLocation")
            .unwrap();
        let schema = description.params_schema.unwrap();
        assert_eq!(schema["oneOf"][1]["maxItems"], 7);
        let example = description.example_request.unwrap()["params"].clone();
        serde_json::from_value::<TrackingRecord>(example.clone()).unwrap();
        for (path, required) in [
            (vec![], &schema["oneOf"][0]["required"]),
            (
                vec!["data"],
                &schema["oneOf"][0]["properties"]["data"]["required"],
            ),
            (
                vec!["data", "kinematics"],
                &schema["oneOf"][0]["properties"]["data"]["properties"]["kinematics"]["required"],
            ),
        ] {
            for key in required.as_array().unwrap() {
                let mut invalid = example.clone();
                let mut value = &mut invalid;
                for part in &path {
                    value = &mut value[*part];
                }
                value.as_object_mut().unwrap().remove(key.as_str().unwrap());
                assert!(
                    serde_json::from_value::<TrackingRecord>(invalid).is_err(),
                    "{key}"
                );
            }
        }
    }
}
