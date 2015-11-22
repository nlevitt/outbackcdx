<webapp>
    <sidebar></sidebar>

    <p>Hello, {collection}</p>

    <script>
        var self = this;
        self.collection = 'nobody';

        riot.route('/collection/*', function (collection) {
            self.update({collection: collection})
        });
    </script>
</webapp>

<sidebar>
    <nav>
        <ul>
            <li each="{ collections }">
                <a href="#collection/{name}">{ name }</a>
            </li>
        </ul>
    </nav>

    <script>
        this.collections = [
            { name: 'agwa' },
            { name: 'pandora '},
            { name: 'wdh' }
        ];
    </script>
</sidebar>