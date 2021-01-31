For a corpus $D$ consisting of $M$ documents each of length $N_{i}$ :
1. Choose $\theta_{i} \sim \operatorname{Dirichlet}(\alpha)$, where $i \in\{1, \ldots, M\}$
2. Choose $\varphi_{k} \sim \operatorname{Dirichlet}(\beta)$, where $k \in\{1, \ldots, K\}$
3. For each of the word positions $i, j$, where $i \in\{1, \ldots, M\}$, and $j \in\left\{1, \ldots, N_{i}\right\}$
   a.  Choose a topic $z_{i, j} \sim \operatorname{Multinomial}\left(\theta_{i}\right)$.
   b.  Choose a word $w_{i, j} \sim \operatorname{Multinomial}\left(\varphi_{z_{i, j}}\right) .$