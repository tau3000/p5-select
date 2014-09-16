#!/usr/local/bin/perl

# 仕様メモ
# - 全体
#   o SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BYの順に記述する必要がある
# - from
# where: 文字列は""で囲む必要がある
#   CSVファイルのみ読み込み可能
#   ファイル名から".csv"を削除した文字列がテーブル名となる。
#   filename as nameでテーブル名にファイル名から.csvを削除したものではなくnameを使える
# - カラム名にはasを使えない。
# - headingの有無にかかわらず、カラム名として#1,#2,...を使える
# - テーブル名は1,2,...またはファイル名から.csvを削除した文字列
# - WHERE, ORDERY BYなどのカラムや演算子の間には空白を入れなければならない
# - ORDER BYのカラム名にカンマは使えない
# - ソート時の型（数値と文字列）は自動判定する
# - WHERE, HAVING時の型（数値と文字列）は自動判定しないので、
#   eq,ne,gt,>,==などのPerl演算子を使って記述する必要がある。and,or,notは使える。
# - 文字列型に対するmax(), min()は文字コードで比較
# - 文字列型に対するsum(), avg()の値は不定（たいてい0になる）
# - カラム名にFROM, WHERE, GROUP, HAVING, ORDERなどは使えない

use strict;
use Getopt::Long;
use Scalar::Util qw/looks_like_number/;
use Time::HiRes qw/gettimeofday tv_interval/;
use Data::Dumper;

our $VERSION = "0.3.4";
our $Opt_debug = '';
our $Opt_version = '';
our $Opt_help = '';
our $Opt_command = '';
our $Opt_heading = '';

Main: {
  Getopt::Long::Configure("bundling");
  my $opts = GetOptions('debug|d' => \$Opt_debug,
			'e=s' => \$Opt_command,
			'heading|H' => \$Opt_heading,
			'version|v' => \$Opt_version,
			'help|h' => \$Opt_help,
		       );

  usage() if $Opt_help;
  version() if $Opt_version;

  if ($Opt_command) {
    # -eオプション
    do_sqls($Opt_command);
  } else {
    # 引数のファイルをSQLとして実行
    my $buf = do { local $/; <> };
    do_sqls($buf);
  }
}

sub version {
  (my $cmd = $0) =~ s{.*/}{};
  die (
       "$cmd $VERSION\n",
       "Copyright (C) 2006-2007 NTT DATA Corporation. All rights reserved.\n",
      );
}

sub usage {
  (my $cmd = $0) =~ s{.*/}{};
  die (
       "Usage: $cmd [OPTION]... [FILE]...\n",
       "\n",
       "Options:\n",
       "    -e statement   read an SELECT statement from command-line instead of files\n",
       "    -H, --heading  input and output all CSV files with heading\n",
       "    -v, --version  print version information\n",
       "    -h, --help     print this help\n",
       "    -d, --debug    debug output\n",
      );
}

sub dprint {
  print STDERR "#DEBUG: @_" if $Opt_debug;
}

###
### SQLの実行
###
sub do_sqls {
  my $buf = shift;
  # `#'で始まる行または空白のみの行はスキップ
  $buf =~ s/^(#.*|\s*)$//mg;
  $buf =~ s/\n/ /g;
  my @sqls = split(/;/, $buf);
  do_sql($_) for @sqls;
}

sub do_sql {
  my $sql = shift;

  # 内容が空の場合は何もしない
  return if $sql =~ /^\s+$/;

  my $t0 = [gettimeofday];
  # SQLとテーブルファイルの読み込み
  my ($query, $tables) = read_sql_and_tables($sql) or die;

  dprint "time_read: ", tv_interval($t0), "\n";
  $t0 = [gettimeofday];
  # 結合(JOIN句)と選択(WHERE句)
  my $records = select_and_join_tables($tables, $query->{where});
  dprint "time_join: ", tv_interval($t0), "\n";
  $t0 = [gettimeofday];

  if ($query->{groupby}) {
    dprint "# group\n";
    # GROUP BY
    my $groups;
    ($records, $groups) = classify_group_records($records, $query->{groupby});
    dprint "time_group: ", tv_interval($t0), "\n";
    $t0 = [gettimeofday];
    # HAVING
    $groups = select_group_records($tables, $records, $groups, $query->{having});
    dprint "time_having: ", tv_interval($t0), "\n";
    $t0 = [gettimeofday];
    # ORDER BY
    $groups = sort_group_records($tables, $records, $groups, $query->{orderby});
    dprint "time_sort: ", tv_interval($t0), "\n";
    $t0 = [gettimeofday];
    # 射影, 出力
    $records = project_and_output_group_records($tables, $records, $groups, $query->{projection}, $query->{to});
    dprint "time_proj: ", tv_interval($t0), "\n";
    $t0 = [gettimeofday];
  } else {
    # ORDER BY
    $records = sort_records($tables, $records, $query->{orderby});
    dprint "time_sort: ", tv_interval($t0), "\n";
    $t0 = [gettimeofday];
    # 射影, 出力
    $records = project_and_output_records($tables, $records, $query->{projection}, $query->{to});
    dprint "time_proj: ", tv_interval($t0), "\n";
  }
}

###
### ソート(ORDER BY句)
###
sub sort_records {
  my ($tables, $records, $sort_columns) = @_;

  return $records unless $sort_columns;

  # 比較関数を生成
  my @exprs;
  for my $col (@{$sort_columns}) {
    my ($table_no, $column_no, $desc) = @{$col}{'table_no', 'column_no', 'desc'};
    my $is_str = $tables->[$table_no]->{column_types}->[$column_no] eq 'str';
    my ($v1, $v2) = $desc ? qw/b a/ : qw/a b/;
    push @exprs, create_column_expr($col, $v1).($is_str?'cmp':'<=>').
      create_column_expr($col, $v2);
  }
  my $sub = eval("sub { " . join(" || ", @exprs) . " }");

  return [ sort $sub @{$records} ];
}

###
### 射影(SELECT句)
###
sub project_and_output_records {
  my ($tables, $records, $projection, $fh) = @_;
  my ($heading, $pivot, $columns) = @{$projection};

  if ($columns->[0]->{column_no} == 0) {
    # "*" の場合
    if ($heading) {
      print $fh join(",", map { $_->{column_names}->[0] } @{$tables}), "\n";
    }
    for my $rj (@{$records}) {
      print $fh join(",", map { $_->[0] } @{$rj}), "\n";
    }
  } else {
    if ($heading) {
      print $fh join(',',
		     map { (s/"/""/g or /[\r\n,]/) ? qq("$_") : $_ }
		     map { $_->{colname} }
		     @{$columns}
		    ), "\n";
    }
    my @exprs = map { create_column_expr($_, "_[0]") } @{$columns};
    my $sub = eval("sub { (" . join(",", @exprs) . ") }");
    for my $rj (@{$records}) {
      print $fh join(',',
		     map { (s/"/""/g or /[\r\n,]/) ? qq("$_") : $_ } &$sub($rj)
		    ), "\n";
    }
  }
}

###
### グループ化(GROUP BY句)
###
sub classify_group_records {
  my ($records, $group_columns) = @_;

  # レコードをグループカラムでソート
  my @exprs =
    map { create_column_expr($_, "_[0]")." cmp ".
	    create_column_expr($_, "_[1]")
	  } @{$group_columns};
  my $sub = eval("sub (\$\$) { " . join(" || ", @exprs) . "}");
  my @records_sorted = sort $sub @{$records};

  # グループごとのbegin, endを算出
  my @groups;
  for my $i (0 .. $#records_sorted) {
    unless (@groups) {
      push @groups, {
		     'begin' => 0,
		     'end' => 1,
		    };
    } elsif (&$sub($records_sorted[$i], $records_sorted[$i-1])) {
      push @groups, {
		     'begin' => $groups[-1]->{end},
		     'end' => $groups[-1]->{end} + 1,
		    };
    } else {
      $groups[-1]->{end}++;
    }
  }

  return (\@records_sorted, \@groups);
}

###
### グループ化したレコードからの選択(HAVING句)
###
sub select_group_records {
  my($tables, $records, $groups, $expr) = @_;

  return $groups unless $expr;

  my $sub = eval("sub { $expr }");
  return [ grep { &$sub($records->[$_->{begin}],
			$tables,
			[ @{$records}[$_->{begin} .. $_->{end} - 1] ])
		} @{$groups} ];
}

###
### グループ化したレコードのソート(ORDER BY句)
###
sub sort_group_records {
  my ($tables, $records, $groups, $sort_columns) = @_;

  return $groups unless $sort_columns;

  my @val_exprs;
  my @cmp_exprs;
  for my $i (0 .. $#{$sort_columns}) {
    my $col = $sort_columns->[$i];
    my ($table_no, $column_no, $desc) =
      @{$col}{'table_no', 'column_no', 'desc'};
    my $is_str = $tables->[$table_no]->{column_types}->[$column_no] eq 'str';
    if ($col->{aggr}) { $is_str = 0; }
    push @val_exprs, create_column_expr($col, "_[0]");
    my ($v1, $v2) = $desc ? qw/b a/ : qw/a b/;
    push @cmp_exprs, '$vals[$'.$v1.']->['.$i.'] '.($is_str?'cmp':'<=>').
      ' $vals[$'.$v2.']->['.$i.']';
  }

  my $val_sub = eval("sub { [" . join(",", @val_exprs) . "] }");
  my @vals = map { &$val_sub($records->[$_->{begin}],
			     $tables,
			     [ @{$records}[$_->{begin} .. $_->{end} - 1] ])
		 } @{$groups};

  my $cmp_sub = eval("sub { " . join(" || ", @cmp_exprs) . " }");

  return [ @{$groups}[ sort $cmp_sub 0 .. $#vals ] ];
}

###
### グループ化したレコードの射影(SELECT句)
###
sub project_and_output_group_records {
  my ($tables, $records, $groups, $projection, $fh) = @_;

  my ($heading, $pivot, $columns) = @{$projection};
  my @exprs = map { create_column_expr($_, "_[0]") } @{$columns};
  my $sub = eval("sub { (" . join(",", @exprs) . ") }");

  if ($pivot) {
    if (@{$columns} != 3) {
      die "projection must be 3 columns for pivot\n";
    }
    my %has_row;
    my %has_col;
    my %ptbl;
    for my $g (@{$groups}) {
      my ($row, $col, $val) =
	&$sub($records->[$g->{begin}],
	      $tables,
	      [ @{$records}[$g->{begin} .. $g->{end} - 1] ]);
      $has_row{$row} = 1;
      $has_col{$col} = 1;
      $ptbl{$row}{$col} = $val;
    }

    my $htop;
    if ($heading) {
      $htop = join('/', map { $_->{colname} } @{$columns}[0,1] );
    }
    # ヘッダの出力
    print $fh join(',', ($htop,
			 map { (s/"/""/g or /[\r\n,]/) ? qq("$_") : $_ }
			 sort keys %has_col )
		  ), "\n";
    my ($tblno, $colno) = @{$columns->[2]}{'table_no', 'column_no'};
    my $is_str = $tables->[$tblno]{column_types}[$colno] eq 'str';
    # ピボットテーブルの出力
    for my $row (sort keys %has_row) {
      print $fh join(',',
		     map { (s/"/""/g or /[\r\n,]/) ? qq("$_") : $_ }
		     ($row, ( map { ( exists($ptbl{$row}{$_})
				      ? $ptbl{$row}{$_}
				      : $is_str ? "" : "0"
				    ) }
			      sort(keys(%has_col)) ) )
		    ), "\n";
    }
  } else {
    # ヘッダの出力
    if ($heading) {
      print $fh join(',',
		     map { (s/"/""/g or /[\r\n,]/) ? qq("$_") : $_ }
		     map { $_->{colname} }
		     @{$columns}
		    ), "\n";
    }
    for my $g (@{$groups}) {
      print $fh join(',',
		     map { (s/"/""/g or /[\r\n,]/) ? qq("$_") : $_ }
		     &$sub($records->[$g->{begin}],
			   $tables,
			   [ @{$records}[$g->{begin} .. $g->{end} - 1] ])
		    ), "\n";
    }
  }
}

sub gr_count {
  my ($tables, $records, $table_no, $column_no) = @_;

  return scalar @{$records};
}

sub gr_sum {
  my ($tables, $records, $table_no, $column_no) = @_;

  my $sum;
  for my $r (@{$records}) {
    $sum += $r->[$table_no]->[$column_no];
  }

  return $sum;
}

sub gr_avg {
  my ($tables, $records, $table_no, $column_no) = @_;

  return gr_sum(@_) / @{$records};
}

sub gr_max {
  my ($tables, $records, $table_no, $column_no) = @_;

  my $max;
  if ($tables->[$table_no]->{column_types}->[$column_no] eq 'str') {
    for my $r (@{$records}) {
      my $v = $r->[$table_no]->[$column_no];
      if (!defined($max) || $max lt $v) {
	$max = $v;
      }
    }
  } else {
    for my $r (@{$records}) {
      my $v = $r->[$table_no]->[$column_no];
      if (!defined($max) || $max < $v) {
	$max = $v;
      }
    }
  }

  return $max;
}

sub gr_min {
  my ($tables, $records, $table_no, $column_no) = @_;

  my $min;
  if ($tables->[$table_no]->{column_types}->[$column_no] eq 'str') {
    for my $r (@{$records}) {
      my $v = $r->[$table_no]->[$column_no];
      if (!defined($min) || $min gt $v) {
	$min = $v;
      }
    }
  } else {
    for my $r (@{$records}) {
      my $v = $r->[$table_no]->[$column_no];
      if (!defined($min) || $min > $v) {
	$min = $v;
      }
    }
  }

  return $min;
}

sub csv_join {
  join(',', map {(s/"/""/g or /[\r\n,]/) ? qq("$_") : $_} @_) . "\n";
}

###
### WHERE句にマッチするレコードについて全テーブルを結合する
### 結合アルゴリズムにはソートマージ法を使用
###
sub select_and_join_tables {
  my ($tables, $cond_expr) = @_;
  my @records_joined;

  if (@{$tables} == 1) {
    my $cond_sub = eval("sub { $cond_expr }");
    for my $r0 (@{$tables->[0]->{records}}) {
      my @rj = ($r0);
      if (!$cond_expr || &$cond_sub(\@rj)) {
	push @records_joined, \@rj;
      }
    }
  } else {
    # 結合条件、選択条件を取得
    my @join_conds;
    my @sel_exprs;
    my @outers;
    for my $expr (split(/\s+&&\s+/, $cond_expr)) {
      if (my ($tblno1, $colno1, $outer1, $op, $tblno2, $colno2, $outer2) =
	  $expr =~ /^\$_\[0\]->\[(\d+)\]->\[(\d+)\]\s*(\(\+\))?\s+(eq|==)\s+\$_\[0\]->\[(\d+)\]->\[(\d+)\]\s*(\(\+\))?$/) {
	# 2個のテーブルに関する等式
	if ($tblno1 == $tblno2) {
	  # 1個のテーブルに関する等式なら選択条件に追加
	  push @{$sel_exprs[$tblno1]}, $expr;
	} elsif ($tblno1 > 0 && $tblno2 > 0) {
	  # 両方とも1個目のテーブルでない場合はエラー
	  die "$expr: invalid condition\n";
	} else {
	  if ($tblno2 == 0) {
	    # $tblno1, $colno1を1個目のテーブルの値にする
	    ($tblno1, $colno1, $outer1, $tblno2, $colno2, $outer2) =
	      ($tblno2, $colno2, $outer2, $tblno1, $colno1, $outer1);
	  }
	  # 結合条件に追加
	  push @{$join_conds[$tblno2]},
	    {
	     'colno1' => $colno1,
	     'colno2' => $colno2,
	     'cmp_op' => $op eq 'eq' ? 'cmp' : '<=>',
	    };
	  $outers[0] ||= $outer1;
	  $outers[$tblno2] ||= $outer2;
	}
      } else {
	# 1個のテーブルに関する等式
	my $tblno;
	while ($expr =~ s/\$_\[0\]->\[(\d+)\]->\[(\d+)\]/\$_[0]->\[$2\]/) {
	  my $n = $1;
	  if (!defined($tblno)) {
	    $tblno = $n;
	  } elsif ($tblno != $n) {
	    die "$expr: invalid condition\n";
	  }
	}
	if (defined($tblno)) {
	  # 選択条件に追加
	  push @{$sel_exprs[$tblno]}, $expr;
	}
      }
    }

    # 結合のメイン処理
    for my $tblno (1 .. $#{$tables}) {
      my @records1;
      if ($tblno == 1) {
	# テーブル1の選択条件でレコードを選択
	if ($sel_exprs[0]) {
	  my $sel_sub = eval("sub { " . join(" && ", @{$sel_exprs[0]}) . " }");
	  @records1 = map { [ $_ ] } grep { &$sel_sub($_) } @{$tables->[0]->{records}};
	} else {
	  @records1 = map { [ $_ ] } @{$tables->[0]->{records}};
	}
      } else {
	# 前回のループのテーブルをコピー
	@records1 = @records_joined;
	@records_joined = ();
      }

      # テーブル2の選択条件でレコードを選択
      my @records2;
      if ($sel_exprs[$tblno]) {
	my $sel_sub = eval("sub { " . join(" && ", @{$sel_exprs[$tblno]}) . " }");
	@records2 = grep { &$sel_sub($_) } @{$tables->[$tblno]->{records}};
      } else {
	@records2 = @{$tables->[$tblno]->{records}};
      }

      # 結合条件中のカラムでテーブル1,2をソート
      # 結合カラムの値の比較関数を算出
      my $valcmp12_sub;
      my $valcmp11_sub;
      my $valcmp22_sub;
      my @colnos1;
      my @colnos2;
      if ($join_conds[$tblno]) {
	my @cmp_exprs1;
	my @cmp_exprs2;
	my @valcmp12_exprs;
	my @valcmp11_exprs;
	my @valcmp22_exprs;
	for my $i (0 .. $#{$join_conds[$tblno]}) {
	  my ($colno1, $colno2, $cmp_op) =
	    @{$join_conds[$tblno]->[$i]}{'colno1', 'colno2', 'cmp_op'};
	  # ソートの比較関数
	  push @cmp_exprs1, '$a->[0]->['.$colno1.'] '.$cmp_op.' $b->[0]->['.$colno1.']';
	  push @cmp_exprs2, '$a->['.$colno2.'] '.$cmp_op.' $b->['.$colno2.']';
	  # 値の比較関数
	  push @valcmp12_exprs, '$_[0]->['.$colno1.'] '.$cmp_op.' $_[1]->['.$colno2.']';
	  push @valcmp11_exprs, '$_[0]->['.$colno1.'] '.$cmp_op.' $_[1]->['.$colno1.']';
	  push @valcmp22_exprs, '$_[0]->['.$colno2.'] '.$cmp_op.' $_[1]->['.$colno2.']';
	}
	# テーブルのソート
	my $cmp_sub1 = eval("sub { " . join(" || ", @cmp_exprs1) . " }");
	@records1 = sort $cmp_sub1 @records1;
	my $cmp_sub2 = eval("sub { " . join(" || ", @cmp_exprs2) . " }");
	@records2 = sort $cmp_sub2 @records2;
	# 値の比較関数
	$valcmp12_sub = eval("sub { " . join(" || ", @valcmp12_exprs) . " }");
	$valcmp11_sub = eval("sub { " . join(" || ", @valcmp11_exprs) . " }");
	$valcmp22_sub = eval("sub { " . join(" || ", @valcmp22_exprs) . " }");
      }

      my $i1 = 0;
      my $i2 = 0;
      my $nr1 = @records1;
      my $nr2 = @records2;
      while ($i1 < $nr1 || $i2 < $nr2) {
	my $i1_end;
	my $i2_end;
	unless ($join_conds[$tblno]) {
	  $i1_end = $nr1;
	  $i2_end = $nr2;
	} else {
	  my $cmp = ( $i2 == $nr2 ? -1 :
		      $i1 == $nr1 ? 1 :
		      &$valcmp12_sub($records1[$i1]->[0], $records2[$i2]) );
	  if ($cmp < 0) {
	    if ($outers[0]) {
	      push @records_joined,
		[
		 @{$records1[$i1]},
		 # 第1要素: 出力が*の場合の出力文字列
		 [ ',' x ($tables->[$tblno]->{ncolumns} - 1) ]
		];
	    }
	    $i1++;
	    next;
	  }
	  elsif ($cmp > 0) {
	    if ($outers[$tblno]) {
	      push @records_joined,
		[
		 # 第1要素: 出力が*の場合の出力文字列
		 # mapの外側の括弧は必要
		 ( map { [ ',' x ($tables->[$_]->{ncolumns} - 1) ] }
		   0 .. $tblno - 1 ),
		 $records2[$i2]
		];
	    }
	    $i2++;
	    next;
	  }
	  # TODO: $i1_end == $nr1 のときに問題ないか確認
	  for ($i1_end = $i1; $i1_end < $nr1; $i1_end++) {
	    last if &$valcmp11_sub($records1[$i1]->[0], $records1[$i1_end]->[0]);
	  }
	  for ($i2_end = $i2; $i2_end < $nr2; $i2_end++) {
	    last if &$valcmp22_sub($records2[$i2], $records2[$i2_end]);
	  }
	}
	# 結合条件を満たすレコードを格納
	for my $r1 (@records1[$i1 .. $i1_end - 1]) {
	  for my $r2 (@records2[$i2 .. $i2_end - 1]) {
	    push @records_joined, [ @{$r1}, $r2 ];
	  }
	}
	$i1 = $i1_end;
	$i2 = $i2_end;
      }
    }
  }

  return \@records_joined;
}

###
### SQLの読み込み
###
sub read_sql_and_tables {
  my $sql = shift;
  $sql =~ s/^\s+//;
  $sql =~ s/\s+$//;

  my $query;

  unless ($sql =~ m/^\s*select\s+(.*?)\s+from\s+(.*?)(?:\s+to\s+(.*?))?(?:\s+where\s+(.*?))?(?:\s+group\s+by\s+(.*?))?(?:\s+having\s+(.*?))?(?:\s+order\s+by\s+(.*?))?\s*$/i) {
    die "syntax error\n";
  }

  my $projection_expr = $1;
  my $tables_expr = $2;
  my $to_expr = $3;
  my $where_expr = $4;
  my $groupby_expr = $5;
  my $having_expr = $6;
  my $orderby_expr = $7;

  dprint("projection\t$projection_expr\n");
  dprint("tables_str\t$tables_expr\n");
  dprint("to\t$to_expr\n");
  dprint("where\t$where_expr\n");
  dprint("groupby\t$groupby_expr\n");
  dprint("having\t$having_expr\n");
  dprint("orderby\t$orderby_expr\n");

  # テーブルファイルの読み込み
  my $tables = read_tables($tables_expr, $projection_expr);

  $query->{projection} = parse_projection_columns($tables, $projection_expr);
  $query->{to} = open_output_file($to_expr);
  $query->{where} = parse_condition($tables, $where_expr);
  $query->{groupby} = parse_columns($tables, $groupby_expr, '');
  if ($query->{groupby} && $projection_expr eq "*") {
    die "Can't output column \"*\" with GROUP BY phrase\n";
  }
  $query->{having} = parse_condition($tables, $having_expr);
  $query->{orderby} = parse_columns($tables, $orderby_expr, 'sort');

  dprint("query->{projection}\t$query->{projection}\n");
  dprint("query->{where}\t$query->{where}\n");
  dprint("query->{groupby}\t$query->{groupby}\n");
  dprint("query->{having}\t$query->{having}\n");
  dprint("query->{orderby}\t$query->{orderby}\n");

  return ($query, $tables);
}

sub open_output_file {
  my $file = shift;

  if ($file eq "") {
    return *STDOUT;
  } else {
    my $fh;
    open $fh, '>', $file or die "$file: $!";
    return $fh;
  }
}

###
### テーブルファイルの読み込み
###
sub read_tables {
  my ($tables_expr, $projection) = @_;

  my @tables;
  my @table_names = split(/\s*,\s*/, $tables_expr);

  for my $table_name (@table_names) {
    # "file" または "file as name heading" を解析して $file, $name に代入
    $table_name =~ /(\S+)(?:\s+as\s+(\S+))?(?:\s+(heading))?/i;
    my $file = $1;
    my $name = $2;
    my $heading = $3 || $Opt_heading;
    unless ($name) {
      # 拡張子.csvとディレクトリ部分を削除
      ($name = $file) =~ s{\.csv$}{}i;
      $name =~ s{^.*[/\\]}{};
    }

    # ファイルの読み込み
    dprint "reading $file\n";
    open my $fh, $file or die "$file: $!";

    my @lines;
    my @records;
    my @column_types;
    my $column_names;
    my $ncolumns;
    while (my $line = <$fh>) {
      $line =~ s/(?:\x0D\x0A|[\x0D\x0A])?$//;
      my $columns = csv_split($line);
      unshift @{$columns}, $projection =~ /^\*/ ? $line : undef;
      unless (defined($ncolumns)) {
	# 1行目
	$ncolumns = @{$columns} - 1;
	if ($heading) {
	  # 1行目のカラムをカラム名にする
	  # レコードへの代入は行わない
	  $column_names = $columns;
	  next;
	}
	$column_names = [ join(',', map { '#'.$_ } 1 .. $ncolumns),
			  map { '#'.$_ } 1 .. $ncolumns ];
      }
      # 読み込んだカラムをレコードに追加する
      push @records, $columns;
      if (@records <= 10) {
	for my $i (1 .. $#{$columns}) {
	  if ($column_types[$i] ne 'str' && !looks_like_number($columns->[$i])) {
	    $column_types[$i] = 'str';
	  }
	}
      }
    }
    push @tables, {
		   'file' => $file,
		   'name' => $name,
		   'ncolumns' => $ncolumns,
		   'column_names' => $column_names,
		   'column_types' => \@column_types,
		   'records' => \@records,
		  };
  }

  return \@tables;
}

sub csv_split {
  my $line = shift;
  # 行末に改行コードがないことが前提条件
  # return [ split(/,/, $line) ];
  $line .= ",";
  my @values = map {/^"(.*)"$/ ? scalar($_ = $1, s/""/"/g, $_) : $_}
    ($line =~ /("[^"]*(?:""[^"]*)*"|[^,]*),/g);
  return \@values;
}

sub parse_projection_columns {
  my ($tables, $columns_expr) = @_;
  my $heading;
  my $pivot;
  for (0 .. 1) {
    $heading = 1 if $columns_expr =~ s/\s+heading$//i;
    $pivot = 1 if $columns_expr =~ s/\s+pivot$//i;
  }
  $heading ||= $Opt_heading;

  return [ $heading, $pivot, parse_columns($tables, $columns_expr, '') ];
}

sub parse_columns {
  my ($tables, $columns_expr, $type) = @_;
  my @columns;

  if ($columns_expr eq '') {
    return undef;
  } else {
    return [ map { create_column($tables, $_, $type) } split(/\s*,\s*/, $columns_expr) ];
  }
}

sub parse_condition {
  my ($tables, $cond_expr) = @_;

  my %perl_op = ( qw/and && or || not !/,
		  map { $_ => $_ } qw/eq ne lt le gt ge == != < <= > >=/ );
  my @res;
  while ($cond_expr =~ s{^(\(\+\)|[()]|".*?"|'.*?'|((?!\(\+\))\S)+)\s*}{}) {
    my $token = $1;
    (my $token_low = $token) =~ tr/A-Z/a-z/;
    # 演算子、カラム名の順番に試す
    my $s = $perl_op{$token_low} ||
      (
       looks_like_number($token) || $token =~ m{\A(\(\+\)|[()]|".*?"|'.*?')\z}
       ? $token
       : create_column_expr(create_column($tables, $token, ''), "_[0]")
      );
    unless ($s) {
      die "$token: invalid token in condition\n";
    }
    push @res, $s;
  }

  return join(" ", @res);
}

sub create_column_expr {
  my ($col, $var) = @_;

  if ($col->{aggr}) {
    return "gr_".$col->{aggr}."(".'$_[1],$_[2],'.$col->{table_no}.
      ",".$col->{column_no}.")";
  } else {
    return '$'.$var.'->['.$col->{table_no}.']->['.$col->{column_no}.']';
  }
}

sub create_column {
  my ($tables, $column_expr, $type) = @_;

  my $desc = ($type eq 'sort' && $column_expr =~ s/\s+desc$//i);

  if (my ($aggr, $colstr) =
      $column_expr =~ /^(count|sum|avg|max|min)\((.*?)\)$/i) {
    # 集約関数
    $aggr =~ tr/A-Z/a-z/;
    (my $laggr = $aggr) =~ tr/a-z/A-Z/;
    my ($table_no, $column_no) = search_column_name($tables, $colstr);
    my $colname = $tables->[$table_no]->{column_names}->[$column_no];
    return {
	    'table_no' => $table_no,
	    'column_no' => $column_no,
	    'desc' => $desc,
	    'aggr' => $aggr,
	    'colname' => $laggr ? "$laggr($colname)" : $colname,
	   };
  } else {
    # カラム値
    my ($table_no, $column_no) = search_column_name($tables, $column_expr);
    my $colname = $tables->[$table_no]->{column_names}->[$column_no];
    return {
	    'table_no' => $table_no,
	    'column_no' => $column_no,
	    'desc' => $desc,
	    'colname' => $colname,
	   };
  }
}

sub search_column_name {
  my ($tables, $column_expr) = @_;
  my $table_no;
  my $column_no;

  my ($table, $column) = $column_expr =~ /^(?:([^.]+)\.)?(\S+?)$/;
  if (defined($table)) {
    # テーブル指定ありの場合
    # テーブル名を番号に変換
    if ($table =~ /^#(\d+)$/) {
      $table_no = $1 - 1;
    } else {
      my @matches = grep { $table eq $tables->[$_]->{name} } (0 .. $#{$tables});
      unless (@matches) {
	die "$table: No such table\n";
      }
      $table_no = $matches[0];
    }
    # カラム名を番号に変換
    if ($column eq "*") {
      $column_no = 0;
    } elsif ($column =~ /^#(\d+)$/) {
      $column_no = $1;
      if ($column_no > $tables->[$table_no]->{ncolumns}) {
	die "$column: No such column\n";
      }
    } else {
      my @matches = grep { $column eq $tables->[$table_no]->{column_names}->[$_] } (0 .. $#{$tables->[$table_no]->{column_names}});
      unless (@matches) {
	die "$column: No such column\n";
      }
      $column_no = $matches[0];
    }
  } else {
    # テーブル指定なしの場合
    # カラム名からテーブル番号、カラム番号を検索
    if ($column eq "*") {
      $table_no = 0;
      $column_no = 0;
    } elsif ($column =~ /^#(\d+)$/) {
      $column_no = $1;
      if ($column_no <= $tables->[0]->{ncolumns}) {
	$table_no = 0;
      } elsif (@{$tables} > 1 && $column_no <= $tables->[1]->{ncolumns}) {
	$table_no = 1;
      } else {
	die "$column: No such column\n";
      }
    } else {
      for my $tno (0 .. $#{$tables}) {
	my @matches = grep { $column eq $tables->[$tno]->{column_names}->[$_] } (0 .. $#{$tables->[$tno]->{column_names}});
	if (@matches) {
	  $table_no = $tno;
	  $column_no = $matches[0];
	  last;
	}
      }
    }
    unless (defined($table_no)) {
      die "$column_expr: No such column\n";
    }
  }

  return ($table_no, $column_no);
}
