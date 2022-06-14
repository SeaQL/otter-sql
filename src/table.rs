// TODO: break into specific traits?
pub trait TableLike {}

pub struct Table {}

pub struct ActiveTable {}

#[cfg(test)]
mod tests {
    use super::{Table, ActiveTable};

    #[test]
    fn create_table() {
        let _ = Table{};
    }

    #[test]
    fn create_active_table() {
        let _ = ActiveTable{};
    }
}
