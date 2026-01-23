use std::{collections::HashSet, ops::Not};

use futures::{FutureExt, StreamExt, future::join3, pin_mut};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedServerName, RoomId, UserId,
	api::federation,
	canonical_json::to_canonical_value,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};
use tuwunel_core::{
	Err, Result, debug_info, err, implement,
	matrix::{PduCount, room_version},
	pdu::PduBuilder,
	state_res,
	utils::{
		self, BoolExt, FutureBoolExt,
		future::{ReadyBoolExt, TryExtExt},
	},
	warn,
};

use super::Service;
use crate::rooms::timeline::RoomMutexGuard;

#[implement(Service)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(%room_id, %user_id)
)]
pub async fn leave(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	reason: Option<&str>,
	remote_leave_now: bool,
	state_lock: &RoomMutexGuard,
) -> Result {
	let is_joined = self
		.services
		.state_cache
		.is_joined(user_id, room_id);

	let is_invited = self
		.services
		.state_cache
		.is_invited(user_id, room_id);

	let is_knocked = self
		.services
		.state_cache
		.is_knocked(user_id, room_id);

	pin_mut!(is_joined, is_invited, is_knocked);

	let can_leave = is_joined.or(is_invited).or(is_knocked).await;

	if !can_leave {
		return Err!(Request(NotFound("You are not in this room.")));
	}

	let is_banned = self.services.metadata.is_banned(room_id);
	let is_disabled = self.services.metadata.is_disabled(room_id);

	pin_mut!(is_banned, is_disabled);

	let need_leave = !is_banned.or(is_disabled).await;

	let need_clean = if need_leave {
		self.do_leave(user_id, room_id, reason, remote_leave_now, state_lock)
			.await
			.inspect_err(|e| warn!(%user_id, %room_id, "Failed to leave room: {e}"))
			.is_err()
	} else {
		warn!(%user_id, %room_id, "Resetting membership state for banned or disabled room");

		true
	};

	if need_clean {
		let default_member_content = RoomMemberEventContent {
			membership: MembershipState::Leave,
			reason: reason.map(ToOwned::to_owned),
			join_authorized_via_users_server: None,
			is_direct: None,
			avatar_url: None,
			displayname: None,
			third_party_invite: None,
			blurhash: None,
		};

		let count = self.services.globals.next_count();
		self.services
			.state_cache
			.update_membership(
				room_id,
				user_id,
				default_member_content,
				user_id,
				None,
				None,
				true,
				PduCount::Normal(*count),
			)
			.await?;
	}

	Ok(())
}

#[implement(Service)]
async fn do_leave(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	reason: Option<&str>,
	remote_leave_now: bool,
	state_lock: &RoomMutexGuard,
) -> Result {
	let member_event = self
		.services
		.state_accessor
		.room_state_get_content::<RoomMemberEventContent>(
			room_id,
			&StateEventType::RoomMember,
			user_id.as_str(),
		)
		.await;

	let dont_have_room = self
		.services
		.state_cache
		.server_in_room(self.services.globals.server_name(), room_id)
		.is_false();

	let not_knocked = self
		.services
		.state_cache
		.is_knocked(user_id, room_id)
		.is_false();

	// Ask a remote server if we don't have this room and are not knocking on it
	if remote_leave_now
		|| (member_event.as_ref().is_err() && dont_have_room.and(not_knocked).await)
	{
		self.remote_leave(user_id, room_id, reason)
			.boxed()
			.await
	} else {
		let event = member_event?;

		self.services
			.timeline
			.build_and_append_pdu(
				PduBuilder::state(user_id.to_string(), &RoomMemberEventContent {
					membership: MembershipState::Leave,
					reason: reason.map(ToOwned::to_owned),
					join_authorized_via_users_server: None,
					is_direct: None,
					..event
				}),
				user_id,
				room_id,
				state_lock,
			)
			.await?;

		Ok(())
	}
}

#[implement(Service)]
#[tracing::instrument(name = "remote", level = "debug", skip_all)]
async fn remote_leave(&self, user_id: &UserId, room_id: &RoomId, reason: Option<&str>) -> Result {
	let mut make_leave_response_and_server =
		Err!(BadServerResponse("No remote server available to assist in leaving {room_id}."));

	let mut servers: HashSet<OwnedServerName> = self
		.services
		.state_cache
		.servers_invite_via(room_id)
		.chain(self.services.state_cache.room_servers(room_id))
		.map(ToOwned::to_owned)
		.collect()
		.await;

	match self
		.services
		.state_cache
		.invite_state(user_id, room_id)
		.await
	{
		| Ok(invite_state) => {
			servers.extend(
				invite_state
					.iter()
					.filter_map(|event| event.get_field("sender").ok().flatten())
					.filter_map(|sender: &str| UserId::parse(sender).ok())
					.map(|user| user.server_name().to_owned()),
			);
		},
		| _ => {
			match self
				.services
				.state_cache
				.knock_state(user_id, room_id)
				.await
			{
				| Ok(knock_state) => {
					servers.extend(
						knock_state
							.iter()
							.filter_map(|event| event.get_field("sender").ok().flatten())
							.filter_map(|sender: &str| UserId::parse(sender).ok())
							.filter_map(|sender| {
								if !self.services.globals.user_is_local(sender) {
									Some(sender.server_name().to_owned())
								} else {
									None
								}
							}),
					);
				},
				| _ => {},
			}
		},
	}

	servers.insert(user_id.server_name().to_owned());
	if let Some(room_id_server_name) = room_id.server_name() {
		servers.insert(room_id_server_name.to_owned());
	}

	debug_info!("servers in remote_leave_room: {servers:?}");

	for remote_server in servers
		.into_iter()
		.filter(|server| !self.services.globals.server_is_ours(server))
	{
		let make_leave_response = self
			.services
			.federation
			.execute(&remote_server, federation::membership::prepare_leave_event::v1::Request {
				room_id: room_id.to_owned(),
				user_id: user_id.to_owned(),
			})
			.await;

		make_leave_response_and_server = make_leave_response.map(|r| (r, remote_server));

		if make_leave_response_and_server.is_ok() {
			break;
		}
	}

	let (make_leave_response, remote_server) = make_leave_response_and_server?;

	let Some(room_version_id) = make_leave_response.room_version else {
		return Err!(BadServerResponse(warn!(
			"No room version was returned by {remote_server} for {room_id}, room version is \
			 likely not supported by tuwunel"
		)));
	};

	if !self
		.services
		.server
		.supported_room_version(&room_version_id)
	{
		return Err!(BadServerResponse(warn!(
			"Remote room version {room_version_id} for {room_id} is not supported by conduwuit",
		)));
	}

	let room_version_rules = room_version::rules(&room_version_id)?;

	let mut event = serde_json::from_str::<CanonicalJsonObject>(make_leave_response.event.get())
		.map_err(|e| {
			err!(BadServerResponse(warn!(
				"Invalid make_leave event json received from {remote_server} for {room_id}: \
				 {e:?}"
			)))
		})?;

	let displayname = self.services.users.displayname(user_id).ok();

	let avatar_url = self.services.users.avatar_url(user_id).ok();

	let blurhash = self.services.users.blurhash(user_id).ok();

	let (displayname, avatar_url, blurhash) = join3(displayname, avatar_url, blurhash).await;

	event.insert(
		"content".into(),
		to_canonical_value(RoomMemberEventContent {
			displayname,
			avatar_url,
			blurhash,
			reason: reason.map(ToOwned::to_owned),
			..RoomMemberEventContent::new(MembershipState::Leave)
		})?,
	);

	event.insert(
		"origin".into(),
		CanonicalJsonValue::String(
			self.services
				.globals
				.server_name()
				.as_str()
				.to_owned(),
		),
	);

	event.insert(
		"origin_server_ts".into(),
		CanonicalJsonValue::Integer(utils::millis_since_unix_epoch().try_into()?),
	);

	event.insert("room_id".into(), CanonicalJsonValue::String(room_id.as_str().into()));

	event.insert("state_key".into(), CanonicalJsonValue::String(user_id.as_str().into()));

	event.insert("sender".into(), CanonicalJsonValue::String(user_id.as_str().into()));

	event.insert("type".into(), CanonicalJsonValue::String("m.room.member".into()));

	let event_id = self
		.services
		.server_keys
		.gen_id_hash_and_sign_event(&mut event, &room_version_id)?;

	state_res::check_pdu_format(&event, &room_version_rules.event_format)?;

	self.services
		.federation
		.execute(&remote_server, federation::membership::create_leave_event::v2::Request {
			room_id: room_id.to_owned(),
			event_id,
			pdu: self
				.services
				.federation
				.format_pdu_into(event.clone(), Some(&room_version_id))
				.await,
		})
		.await?;

	Ok(())
}
