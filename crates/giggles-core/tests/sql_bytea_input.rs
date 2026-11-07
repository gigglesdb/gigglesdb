use giggles_core::{
    Admin, CoreError, Limits,
    relational::{SqlError as E, Value as V},
};
#[test]
fn bytea_consumers_pg18_fixture() {
    let path = std::env::temp_dir().join(format!("bytea-candidate-{}", std::process::id()));
    let a = Admin::create(path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    {
        let input = String::from_utf8(vec![]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![])]);
    }
    {
        let input = String::from_utf8(vec![]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![])]);
    }
    {
        let input = String::from_utf8(vec![97, 98, 99]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(
            got.unwrap().rows[0].values,
            vec![V::Bytes(vec![97, 98, 99])]
        );
    }
    {
        let input = String::from_utf8(vec![97, 98, 99]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(
            got.unwrap().rows[0].values,
            vec![V::Bytes(vec![97, 98, 99])]
        );
    }
    {
        let input = String::from_utf8(vec![92, 120]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![])]);
    }
    {
        let input = String::from_utf8(vec![92, 120]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![])]);
    }
    {
        let input = String::from_utf8(vec![92, 120, 48, 48, 65, 98, 102, 102]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(
            got.unwrap().rows[0].values,
            vec![V::Bytes(vec![0, 171, 255])]
        );
    }
    {
        let input = String::from_utf8(vec![92, 120, 48, 48, 65, 98, 102, 102]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(
            got.unwrap().rows[0].values,
            vec![V::Bytes(vec![0, 171, 255])]
        );
    }
    {
        let input = String::from_utf8(vec![195, 169, 240, 159, 152, 128]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(
            got.unwrap().rows[0].values,
            vec![V::Bytes(vec![195, 169, 240, 159, 152, 128])]
        );
    }
    {
        let input = String::from_utf8(vec![195, 169, 240, 159, 152, 128]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(
            got.unwrap().rows[0].values,
            vec![V::Bytes(vec![195, 169, 240, 159, 152, 128])]
        );
    }
    {
        let input = String::from_utf8(vec![92, 120, 49]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidParameterValue)));
    }
    {
        let input = String::from_utf8(vec![92, 120, 49]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidParameterValue)));
    }
    {
        let input = String::from_utf8(vec![92, 120, 122, 122]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidParameterValue)));
    }
    {
        let input = String::from_utf8(vec![92, 120, 122, 122]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidParameterValue)));
    }
    {
        let input = String::from_utf8(vec![92]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 92]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![92])]);
    }
    {
        let input = String::from_utf8(vec![92, 92]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![92])]);
    }
    {
        let input = String::from_utf8(vec![92, 48, 48, 48]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![0])]);
    }
    {
        let input = String::from_utf8(vec![92, 48, 48, 48]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![0])]);
    }
    {
        let input = String::from_utf8(vec![92, 51, 55, 55]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![255])]);
    }
    {
        let input = String::from_utf8(vec![92, 51, 55, 55]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![255])]);
    }
    {
        let input = String::from_utf8(vec![92, 49]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 49]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 49, 50]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 49, 50]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 52, 48, 48]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 52, 48, 48]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 49, 50, 56]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 49, 50, 56]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got, Err(CoreError::Sql(E::InvalidTextRepresentation)));
    }
    {
        let input = String::from_utf8(vec![92, 49, 50, 51, 52]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![83, 52])]);
    }
    {
        let input = String::from_utf8(vec![92, 49, 50, 51, 52]).unwrap();
        let got = s.execute("SELECT $1::text::bytea", &[V::Text(input)], None);
        assert_eq!(got.unwrap().rows[0].values, vec![V::Bytes(vec![83, 52])]);
    }
    assert_eq!(
        s.execute("SELECT $1::text::bytea", &[V::Text("a\0b".into())], None),
        Err(CoreError::Sql(E::CharacterNotInRepertoire))
    );
    assert_eq!(
        s.execute("SELECT $1::bytea", &[V::Bytes(vec![0, 255, 92])], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Bytes(vec![0, 255, 92])]
    );
    assert_eq!(
        s.execute("SELECT $1::text::bytea", &[V::Null], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Null]
    );
}
#[test]
fn nested_contexts() {
    use giggles_core::{
        Admin, CoreError, Limits,
        relational::{SqlError as E, Value as V},
    };
    let a = Admin::create(
        std::env::temp_dir().join(format!("bytea-nested-{}", std::process::id())),
        Limits::default(),
    )
    .unwrap();
    let mut s = a.sql_session();
    let got=s.execute("SELECT octet_length($1::text::bytea), encode(substring($1::text::bytea FROM 1 FOR 2),'hex')",&[V::Text("\\101bc".into())],None).unwrap();
    assert_eq!(got.rows[0].values, vec![V::Int(3), V::Text("4162".into())]);
    assert_eq!(
        s.execute(
            "SELECT overlay($1::text::bytea placing NULL::bytea from 1)",
            &[V::Text("\\xgg".into())],
            None
        ),
        Err(CoreError::Sql(E::InvalidParameterValue))
    );
}
