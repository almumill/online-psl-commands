Exp1:
Base
[      obs           \    t1    ] time [p1] evaluate
[      obs           \    t1     t2    ] time [p1, p2] evaluate
[      obs           \    t1     t2     t3  ] time [p1, p2, p3] evaluate

Online
0[      obs           \    t0    ] time [p0] evaluate
1[      obs                t0  \   t1    ] time [p0, p1] evaluate
2[      obs                t0      t1  \   t2  ] time [p0, p1, p2] evaluate

Exp2: 
Base
0[      obs           \    t0    ] time [p0] evaluate
1[      obs                t0  \   t1    ] time [p0, p1] evaluate
2[      obs                t0      t1  \   t2  ] time [p0, p1, p2] evaluate

Online
[      obs           \   t1    ] time [p1] evaluate
[      obs               t1   \  t2    ] time [p1, p2] evaluate
[      obs               t1      t2  \   t3  ] time [p1, p2, p3] evaluate
