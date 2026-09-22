//! The datagram side of the call: handing packets to str0m, telling RTP apart, the
//! per-call network counters, and the routable host address.

use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr, UdpSocket},
    time::Instant,
};

use str0m::{
    net::{Protocol, Receive},
    Input, Rtc,
};

/// Datagram counters by kind (RFC 7983 first-byte demux) and source, plus the outbound
/// "microphone" writes — enough to tell "the network lost it" from "str0m dropped it".
#[derive(Default, Debug)]
pub(crate) struct NetStats {
    pub(crate) stun: u64,
    pub(crate) dtls: u64,
    pub(crate) rtp: u64,
    pub(crate) other: u64,
    pub(crate) sources: BTreeMap<SocketAddr, u64>,
    pub(crate) rtp_by_ssrc_pt: BTreeMap<(u32, u8), u64>,
    pub(crate) mic_sent: u64,
    pub(crate) mic_failed: u64,
}

impl NetStats {
    pub(crate) fn count(&mut self, datagram: &[u8], source: SocketAddr) {
        match datagram.first() {
            Some(0..=3) => self.stun += 1,
            Some(20..=63) => self.dtls += 1,
            Some(128..=191) => {
                self.rtp += 1;
                // Tally RTP (not RTCP) by SSRC/PT: which streams the peer actually sends on.
                if let (Some(_), Some(ssrc)) = (rtp_seq(datagram), datagram.get(8..12)) {
                    let pt = datagram[1] & 0x7f;
                    let ssrc = u32::from_be_bytes([ssrc[0], ssrc[1], ssrc[2], ssrc[3]]);
                    *self.rtp_by_ssrc_pt.entry((ssrc, pt)).or_default() += 1;
                }
            }
            _ => self.other += 1,
        }
        *self.sources.entry(source).or_default() += 1;
    }
}

/// Hand one received datagram to str0m.
pub(crate) fn feed(rtc: &mut Rtc, datagram: &[u8], source: SocketAddr, local: SocketAddr) -> Result<(), String> {
    let contents = datagram.try_into().map_err(|e| format!("bad datagram: {e}"))?;
    let recv = Receive { proto: Protocol::Udp, source, destination: local, contents };
    rtc.handle_input(Input::Receive(Instant::now(), recv)).map_err(|e| e.to_string())
}

/// The sequence number of an RTP packet, or `None` for anything else — STUN, DTLS, and
/// RTCP (whose second byte, marker bit included, falls in 192..=223; RFC 5761 §4).
pub(crate) fn rtp_seq(datagram: &[u8]) -> Option<u16> {
    let (&first, &second) = (datagram.first()?, datagram.get(1)?);
    if !(128..=191).contains(&first) || (192..=223).contains(&second) {
        return None;
    }
    Some(u16::from_be_bytes([*datagram.get(2)?, *datagram.get(3)?]))
}

/// The default-route local IP: connect a throwaway UDP socket to a public address (sends
/// nothing) and read back which interface the OS would use.
pub(crate) fn routable_ip() -> Result<IpAddr, String> {
    let probe = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    probe.connect("1.1.1.1:80").map_err(|e| format!("probe route: {e}"))?;
    Ok(probe.local_addr().map_err(|e| e.to_string())?.ip())
}

#[cfg(test)]
mod tests {
    use super::rtp_seq;

    #[test]
    fn rtp_is_told_apart_from_rtcp_stun_and_dtls() {
        let rtp = [0x80, 0x6f, 0x1c, 0x68, 0, 0, 0, 0, 0, 0, 0, 1]; // PT 111, seq 7272
        assert_eq!(rtp_seq(&rtp), Some(7272));
        assert_eq!(rtp_seq(&[0x81, 0xc9, 0, 7]), None); // RTCP receiver report (PT 201)
        assert_eq!(rtp_seq(&[0x00, 0x01, 0, 0]), None); // STUN
        assert_eq!(rtp_seq(&[0x16, 0xfe, 0xfd, 0]), None); // DTLS
    }
}
