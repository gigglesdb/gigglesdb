use giggles_consensus::{MAX_FRAME, transport::*};
use raft::eraftpb::{Message, MessageType};
use tokio::io::AsyncWriteExt;

#[test]
fn peer_frames_bind_cluster_identity_and_format() {
    let mut m = Message {
        from: 1,
        to: 2,
        term: 7,
        ..Default::default()
    };
    m.set_msg_type(MessageType::MsgHeartbeat);
    let bytes = encode_message(42, &m).unwrap();
    assert_eq!(decode_message(42, &bytes).unwrap(), m);
    assert!(decode_message(43, &bytes).is_err());
    for len in 0..24 {
        assert!(decode_message(42, &bytes[..len]).is_err());
    }
    let mut bad = bytes;
    bad[0] ^= 1;
    assert!(decode_message(42, &bad).is_err());
}

#[tokio::test]
async fn network_frame_exact_maximum_and_oversize_prefix() {
    let (mut tx, mut rx) = tokio::io::duplex(4096);
    let writer = tokio::spawn(async move {
        write_frame(&mut tx, &vec![7; MAX_FRAME]).await.unwrap();
    });
    let bytes = read_frame(&mut rx).await.unwrap();
    assert_eq!(bytes.len(), MAX_FRAME);
    assert!(bytes.iter().all(|b| *b == 7));
    writer.await.unwrap();
    for len in [0, MAX_FRAME as u32 + 1, u32::MAX] {
        let (mut tx, mut rx) = tokio::io::duplex(4);
        tx.write_u32(len).await.unwrap();
        assert!(read_frame(&mut rx).await.is_err());
    }
    let (mut tx, _rx) = tokio::io::duplex(1);
    assert!(write_frame(&mut tx, &[]).await.is_err());
    assert!(write_frame(&mut tx, &vec![0; MAX_FRAME + 1]).await.is_err());
}
