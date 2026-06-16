pub const INVITATION_SIGNUP_INVITER_ORDER_TYPE: &str = "invite_inviter";
pub const INVITATION_SIGNUP_INVITEE_ORDER_TYPE: &str = "invite_invitee";
pub const TOPUP_ORDER_TYPE: &str = "topup";
pub const SUBSCRIPTION_ORDER_TYPE: &str = "subscription";
pub const REDEMPTION_ORDER_TYPE: &str = "redemption";
pub const DEFAULT_QUOTA_UNITS_PER_YUAN: i64 = 500_000;
pub const CENTS_PER_YUAN: i64 = 100;

pub fn is_order_rebate_order_type(order_type: &str) -> bool {
    matches!(
        order_type,
        TOPUP_ORDER_TYPE | SUBSCRIPTION_ORDER_TYPE | REDEMPTION_ORDER_TYPE
    )
}

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

    #[test]
    fn identifies_order_rebate_order_types() {
        assert!(is_order_rebate_order_type(TOPUP_ORDER_TYPE));
        assert!(is_order_rebate_order_type(SUBSCRIPTION_ORDER_TYPE));
        assert!(is_order_rebate_order_type(REDEMPTION_ORDER_TYPE));
        assert!(!is_order_rebate_order_type(
            INVITATION_SIGNUP_INVITER_ORDER_TYPE
        ));
        assert!(REDEMPTION_ORDER_TYPE.len() <= 20);
    }
}
