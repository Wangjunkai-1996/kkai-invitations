pub const INVITATION_SIGNUP_INVITER_ORDER_TYPE: &str = "invite_inviter";
pub const INVITATION_SIGNUP_INVITEE_ORDER_TYPE: &str = "invite_invitee";

pub fn is_invitation_signup_reward_order_type(order_type: &str) -> bool {
    matches!(
        order_type,
        INVITATION_SIGNUP_INVITER_ORDER_TYPE | INVITATION_SIGNUP_INVITEE_ORDER_TYPE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_invitation_signup_reward_order_types() {
        assert!(is_invitation_signup_reward_order_type(
            INVITATION_SIGNUP_INVITER_ORDER_TYPE
        ));
        assert!(is_invitation_signup_reward_order_type(
            INVITATION_SIGNUP_INVITEE_ORDER_TYPE
        ));
        assert!(!is_invitation_signup_reward_order_type("topup"));
        assert!(INVITATION_SIGNUP_INVITER_ORDER_TYPE.len() <= 20);
        assert!(INVITATION_SIGNUP_INVITEE_ORDER_TYPE.len() <= 20);
    }
}
