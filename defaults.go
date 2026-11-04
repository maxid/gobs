package gobs

// Default registers the reference-first adapters used by application
// distributions. Composition roots that need a subset can call the individual
// Register* functions instead.
func Default(r *Registry) error {
	if r == nil {
		return ErrInvalidAdapter
	}
	for _, register := range []func(*Registry) error{
		RegisterGNety,
		RegisterGMQX,
		RegisterGModbus,
		RegisterGBus,
		RegisterGJob,
	} {
		if err := register(r); err != nil {
			return err
		}
	}
	return nil
}
