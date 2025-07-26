package tool

func TryAll(funcs ...func() error) (err error) {
	for _, fn := range funcs {
		err = fn()
		if err != nil {
			return err
		}
	}
	return nil
}
