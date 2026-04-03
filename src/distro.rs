use flow::FlowInstance;

/// Apply build-time selected distro registrations to the given FlowInstance.
pub fn register_selected_distro(instance: &FlowInstance) {
    veloflux_sdv::register(instance);
}
