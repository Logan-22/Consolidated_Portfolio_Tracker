export default class FormToTable {
    constructor(form_selector, form_data_id, table_container_id, custom_fields = [], exclude_fields = [], column_types = {}, immutable_fields = []) {
        this.form = document.querySelector(form_selector)
        this.form_data_id = form_data_id
        this.table_container = document.querySelector(table_container_id)
        this.table = null
        this.final_submit_button = null
        this.rootStyle = getComputedStyle(document.documentElement)
        this.custom_fields = custom_fields
        this.exclude_fields = exclude_fields
        this.column_types = column_types
        this.immutable_fields = immutable_fields
        this._init()
    }

    _init() {
        this.final_submit_button = document.createElement('button')
        this.final_submit_button.textContent = 'Submit'
        this.final_submit_button.style.display = 'none'

        // Append button after table
        this.table_container.insertAdjacentElement('afterend', this.final_submit_button)

        this.form.addEventListener('submit', async (e) => {
            e.preventDefault()
            const form_data = new FormData(this.form)
            const row_data = {}
            form_data.forEach((value, key) => { row_data[key] = value })

            if (!this.table) {
                this._init_table(row_data)
            }
            await this._add_row(row_data)
            this.form.reset()
        })
    }

    _init_table(data) {
        this.table = document.createElement('table')
        this.table.style.borderCollapse = 'collapse'
        this.table.style.marginTop = '16px'
        this.table.style.width = '100%'

        const thead = document.createElement('thead')
        const header_row = document.createElement('tr')
        const all_keys = [
            ...Object.keys(data).filter(key => !this.exclude_fields.includes(key)),
            ...this.custom_fields.map(field => field.name)
        ]
        this.headers = all_keys
        all_keys.forEach(key => {
            const th = document.createElement('th')
            // Title Case
            th.textContent = key.replace(/_/g, ' ').replace(/\w\S*/g, w => w[0].toUpperCase() + w.slice(1).toLowerCase())
            header_row.appendChild(th)
        })
        // Delete row action
        const th_actions = document.createElement('th')
        th_actions.textContent = 'Delete entry?'
        header_row.appendChild(th_actions)
        thead.appendChild(header_row)

        this.table.appendChild(thead)
        this.table.appendChild(document.createElement('tbody'))

        this.table_container.appendChild(this.table)
        this.final_submit_button.style.display = 'inline-block'
        this.final_submit_button.style.marginTop = '8px'
    }

    async _add_row(data) {
        const tbody = document.querySelector('tbody')
        const tr = document.createElement('tr')

        // Populate Custom Fields
        for (const field of this.custom_fields) {
            if (field.populate) {
                data[field.name] = await field.populate(data)
            } else {
                data[field.name] = ''
            }
        }

        //Populate Form Fields
        const filtered_columns_data = Object.fromEntries(
            Object.entries(data).filter(([key]) => !this.exclude_fields.includes(key))
        )
        Object.entries(filtered_columns_data).forEach(([key, value]) => {
            const td = document.createElement('td')
            td.textContent = value
            td.dataset.original = value
            td.dataset.key = key
            td.style.border = `1px solid ${this.rootStyle.getPropertyValue('--accent')}`
            td.style.padding = '8px'

            if (!this.immutable_fields.includes(key)) {
                td.contentEditable = 'true'
                td.addEventListener('input', () => {
                    td.style.backgroundColor = td.textContent.trim() === td.dataset.original ? '' : this.rootStyle.getPropertyValue('--updated').trim()
                })
            } else {
                td.contentEditable = 'false'
                td.style.backgroundColor = this.rootStyle.getPropertyValue('--immutable').trim()
            }


            tr.appendChild(td)
        })

        // Delete Button
        const td_action = document.createElement('td')
        td_action.style.border = `1px solid ${this.rootStyle.getPropertyValue('--accent')}`
        td_action.style.padding = '8px'
        const delete_button = document.createElement('button')
        delete_button.textContent = 'X'
        delete_button.addEventListener('click', () => tr.remove())

        td_action.appendChild(delete_button)
        tr.appendChild(td_action)
        tbody.appendChild(tr)
    }

    _final_submit() {
        const all_rows = document.querySelectorAll('tbody tr')
        const form_data = new FormData()
        const rows = []
        let validation_failed = false

        all_rows.forEach(row => {
            const cells = row.querySelectorAll('td:not(:last-child)')// to ignore delete entry cell
            const row_data = {}
            cells.forEach(cell => {
                const key = cell.dataset.key
                const value = cell.textContent.trim()
                const expected = this.column_types[key]

                //Reset outline
                cell.style.outline = ''

                //Validate based on type
                let valid = true
                if (expected.type) {
                    switch (expected.type) {
                        case 'number':
                            valid = /^-?\d+(\.\d+)?$/.test(value.trim()); break;
                        case 'date':
                            valid = !isNaN(Date.parse(value)); break;
                        case 'string':
                            valid = value.length > 0; break
                        case 'number_gt_0':
                            if (/^-?\d+(\.\d+)?$/.test(value.trim())) {
                                valid = parseFloat(value) > 0
                            } else {
                                valid = false
                            }break;
                    }
                }

                if (valid && expected.allowed && !expected.allowed.includes(value)) {
                    valid = false
                }
                if (!valid) {
                    validation_failed = true
                    cell.style.outline = `2px solid ${this.rootStyle.getPropertyValue('--error')}`
                }
                row_data[key.toUpperCase()] = value
            })
            rows.push(row_data)
        })

        if (validation_failed) {
            alert('Some cells contain invalid values. Please correct the highlighted cells')
            return null
        }

        form_data.append(this.form_data_id, JSON.stringify(rows))
        return form_data
    }
}