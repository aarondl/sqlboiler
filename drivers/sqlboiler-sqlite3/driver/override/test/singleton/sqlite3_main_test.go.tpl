var rgxSQLitekey = regexp.MustCompile(`(?mi)((,\n)?\s+foreign key.*?\n)+`)

type sqliteTester struct {
	dbConn *sql.DB

	dbName	string
	testDBName string
}

func init() {
	dbMain = &sqliteTester{}
}

func (s *sqliteTester) setup() error {
	var err error

    s.dbName = viper.GetString("sqlite3.dbname")
    if len(s.dbName) == 0 {
        return errors.New("no dbname specified")
    }
	if !filepath.IsAbs(s.dbName) {
		if configFile := viper.ConfigFileUsed(); configFile != "" {
			s.dbName = filepath.Join(filepath.Dir(configFile), s.dbName)
		}
	}

	s.testDBName = filepath.Join(os.TempDir(), fmt.Sprintf("boil-sqlite3-%d.sql", rand.Int()))

	dumpCmd := exec.Command("sqlite3", "-cmd", ".dump", s.dbName)
	createCmd := exec.Command("sqlite3", s.testDBName)

	r, w := io.Pipe()
	dumpCmd.Stdout = w
	createCmd.Stdin = newFKeyDestroyer(rgxSQLitekey, r)

	if err = dumpCmd.Start(); err != nil {
		return errors.Wrap(err, "failed to start sqlite3 dump command")
	}
	if err = createCmd.Start(); err != nil {
		return errors.Wrap(err, "failed to start sqlite3 create command")
	}

	if err = dumpCmd.Wait(); err != nil {
		fmt.Println(err)
		return errors.Wrap(err, "failed to wait for sqlite3 dump command")
	}

	w.Close() // After dumpCmd is done, close the write end of the pipe

	if err = createCmd.Wait(); err != nil {
		fmt.Println(err)
		return errors.Wrap(err, "failed to wait for sqlite3 create command")
	}

	return nil
}

func (s *sqliteTester) teardown() error {
	if s.dbConn != nil {
		s.dbConn.Close()
	}

	return os.Remove(s.testDBName)
}

func (s *sqliteTester) conn() (*sql.DB, error) {
	if s.dbConn != nil {
        return s.dbConn, nil
	}

	var err error
	s.dbConn, err = sql.Open("sqlite", fmt.Sprintf("file:%s?cache=shared&_loc=UTC", s.testDBName))
        if err != nil {
        return nil, err
	}

	return s.dbConn, nil
}
